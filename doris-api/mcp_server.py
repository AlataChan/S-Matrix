"""
Minimal MCP-compatible stdio wrapper over the S-Matrix REST API.
"""
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import requests


class SmatrixApiClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def query_natural(self, question: str) -> Dict[str, Any]:
        return self._request("POST", "/api/query/natural", json={"query": question})

    def list_tables(self) -> Any:
        registry = self._request("GET", "/api/table-registry")
        if registry.get("tables"):
            return registry["tables"]
        fallback = self._request("GET", "/api/tables")
        return fallback.get("tables", [])

    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/tables/{table_name}/schema")

    def upload_excel(
        self,
        file_path: str,
        description: str = "",
        table_name: str | None = None,
    ) -> Dict[str, Any]:
        path = Path(file_path).expanduser().resolve()
        resolved_table_name = table_name or self._derive_table_name(path)
        with path.open("rb") as handle:
            result = self._request(
                "POST",
                "/api/upload",
                data={"table_name": resolved_table_name, "create_table": "true"},
                files={"file": (path.name, handle, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            )

        if description:
            self._request(
                "PUT",
                f"/api/table-registry/{resolved_table_name}",
                json={"display_name": resolved_table_name, "description": description},
            )

        return result

    def _derive_table_name(self, path: Path) -> str:
        stem = path.stem.strip().lower()
        sanitized = "".join(character if character.isalnum() or character == "_" else "_" for character in stem)
        return sanitized.strip("_") or "uploaded_table"

    def _request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        headers = kwargs.pop("headers", {})
        headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "X-API-Key": self.api_key,
            }
        )
        response = requests.request(method, f"{self.base_url}{path}", headers=headers, timeout=120, **kwargs)
        response.raise_for_status()
        return response.json()


def build_api_client() -> SmatrixApiClient:
    return SmatrixApiClient(
        base_url=os.getenv("SMATRIX_API_URL", "http://localhost:38018"),
        api_key=os.getenv("SMATRIX_API_KEY", ""),
    )


TOOLS = [
    {
        "name": "query_natural",
        "description": "Run a natural-language query and return generated SQL plus result rows.",
        "inputSchema": {
            "type": "object",
            "properties": {"question": {"type": "string"}},
            "required": ["question"],
        },
    },
    {
        "name": "list_tables",
        "description": "List available business tables and descriptions.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_table_schema",
        "description": "Get schema details for a specific table.",
        "inputSchema": {
            "type": "object",
            "properties": {"table_name": {"type": "string"}},
            "required": ["table_name"],
        },
    },
    {
        "name": "upload_excel",
        "description": "Upload an Excel file through the REST API and create a new table.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string"},
                "description": {"type": "string"},
                "table_name": {"type": "string"},
            },
            "required": ["file_path"],
        },
    },
]


def handle_jsonrpc_request(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    request_id = payload.get("id")
    method = payload.get("method")
    params = payload.get("params") or {}

    try:
        if request_id is None and method and method.startswith("notifications/"):
            return None

        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "serverInfo": {"name": "smatrix", "version": "1.0.0"},
                    "capabilities": {"tools": {}},
                },
            }

        if method == "tools/list":
            return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": TOOLS}}

        if method == "tools/call":
            name = params.get("name")
            arguments = params.get("arguments") or {}
            client = build_api_client()
            if name == "query_natural":
                result = client.query_natural(arguments["question"])
            elif name == "list_tables":
                result = client.list_tables()
            elif name == "get_table_schema":
                result = client.get_table_schema(arguments["table_name"])
            elif name == "upload_excel":
                result = client.upload_excel(
                    arguments["file_path"],
                    arguments.get("description", ""),
                    arguments.get("table_name"),
                )
            else:
                raise ValueError(f"Unknown tool: {name}")

            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}],
                    "structuredContent": result,
                },
            }

        if method == "ping":
            return {"jsonrpc": "2.0", "id": request_id, "result": {"ok": True}}

        raise ValueError(f"Unsupported method: {method}")
    except Exception as error:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32000, "message": str(error)},
        }


def main() -> None:
    for raw_line in sys.stdin:
        message = raw_line.strip()
        if not message:
            continue
        response = handle_jsonrpc_request(json.loads(message))
        if response is None:
            continue
        sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
