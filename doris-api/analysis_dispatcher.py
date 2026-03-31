"""
Report delivery channels for analysis results.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import os
import socket
from typing import Any, Dict
from urllib.parse import urlsplit

import httpx


logger = logging.getLogger(__name__)


class AnalysisDispatcher:
    def __init__(self):
        self.ws_connections: Dict[str, Any] = {}
        self.max_ws = int(os.getenv("ANALYST_WS_MAX_CONNECTIONS", "50"))
        self.webhook_timeout = int(os.getenv("ANALYST_WEBHOOK_TIMEOUT", "30"))
        self.webhook_retry = int(os.getenv("ANALYST_WEBHOOK_RETRY", "2"))
        self.webhook_backoff = float(os.getenv("ANALYST_WEBHOOK_BACKOFF", "1"))

    async def dispatch(self, report: Dict[str, Any], delivery_config: Dict[str, Any] | None) -> None:
        channels = (delivery_config or {}).get("channels") or []
        for channel in channels:
            try:
                if channel.get("type") == "webhook":
                    await self._send_webhook(report, channel)
                elif channel.get("type") == "websocket":
                    await self._push_ws(report)
            except Exception as exc:
                logger.warning("dispatch to %s failed: %s", channel.get("type"), exc)

    async def _send_webhook(self, report: Dict[str, Any], channel: Dict[str, Any]) -> None:
        fmt = channel.get("format", "generic")
        payload = self._format_payload(report, fmt)
        headers = {"Content-Type": "application/json"}
        if channel.get("webhook_token"):
            headers["Authorization"] = f"Bearer {channel['webhook_token']}"
        self._validate_webhook_url(channel["webhook_url"])

        async with httpx.AsyncClient(timeout=self.webhook_timeout) as client:
            for attempt in range(self.webhook_retry + 1):
                try:
                    response = await client.post(channel["webhook_url"], json=payload, headers=headers)
                    response.raise_for_status()
                    return
                except Exception as exc:
                    if attempt >= self.webhook_retry:
                        logger.error("webhook delivery failed after %d attempts: %s", attempt + 1, exc)
                        return
                    await asyncio.sleep(self.webhook_backoff * (2**attempt))

    def _format_payload(self, report: Dict[str, Any], fmt: str) -> Dict[str, Any]:
        if fmt == "slack":
            return self._format_slack(report)
        if fmt == "dingtalk":
            return self._format_dingtalk(report)
        return report

    def _format_slack(self, report: Dict[str, Any]) -> Dict[str, Any]:
        table_names = self._format_table_names(report.get("table_names"))
        return {
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": "Analysis Report"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": report.get("summary") or "Analysis completed."}},
                {
                    "type": "context",
                    "elements": [
                        {"type": "mrkdwn", "text": f"Report ID: {report.get('id', '-') }"},
                        {"type": "mrkdwn", "text": f"Tables: {table_names}"},
                    ],
                },
            ]
        }

    def _format_dingtalk(self, report: Dict[str, Any]) -> Dict[str, Any]:
        summary = report.get("summary") or "Analysis completed."
        table_names = self._format_table_names(report.get("table_names"))
        return {
            "msgtype": "markdown",
            "markdown": {
                "title": "Analysis Report",
                "text": f"### Analysis Report\n\n{summary}\n\n- Report ID: {report.get('id', '-')}\n- Tables: {table_names}",
            },
        }

    async def ws_connect(self, ws: Any, client_id: str) -> None:
        await ws.accept()
        if len(self.ws_connections) >= self.max_ws:
            await ws.close(code=1013, reason="Too many websocket clients")
            return
        self.ws_connections[client_id] = ws

    async def ws_disconnect(self, client_id: str) -> None:
        self.ws_connections.pop(client_id, None)

    async def _push_ws(self, report: Dict[str, Any]) -> None:
        stale_clients = []
        for client_id, ws in list(self.ws_connections.items()):
            try:
                await ws.send_json(report)
            except Exception:
                stale_clients.append(client_id)

        for client_id in stale_clients:
            ws = self.ws_connections.pop(client_id, None)
            if ws is None:
                continue
            try:
                await ws.close(code=1011, reason="WebSocket delivery failed")
            except Exception:
                pass

    def _format_table_names(self, value: Any) -> str:
        if isinstance(value, (list, tuple, set)):
            items = [str(item).strip() for item in value if str(item).strip()]
            return ", ".join(items) if items else "-"
        text = str(value or "").strip()
        return text or "-"

    def _validate_webhook_url(self, url: str) -> None:
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("webhook_url must use http or https")

        hostname = parsed.hostname
        if not hostname:
            raise ValueError("webhook_url must include a hostname")

        lowered = hostname.lower()
        if lowered == "localhost" or lowered.endswith(".localhost"):
            raise ValueError("webhook_url cannot target localhost")

        try:
            candidate_ips = [ipaddress.ip_address(hostname)]
        except ValueError:
            try:
                addrinfo = socket.getaddrinfo(hostname, parsed.port or 443, type=socket.SOCK_STREAM)
            except socket.gaierror:
                addrinfo = []
            candidate_ips = []
            for entry in addrinfo:
                try:
                    candidate_ips.append(ipaddress.ip_address(entry[4][0]))
                except ValueError:
                    continue

        for candidate_ip in candidate_ips:
            if (
                candidate_ip.is_private
                or candidate_ip.is_loopback
                or candidate_ip.is_link_local
                or candidate_ip.is_multicast
                or candidate_ip.is_reserved
                or candidate_ip.is_unspecified
            ):
                raise ValueError("webhook_url cannot target private or reserved network addresses")
