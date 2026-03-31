"""
Shared APScheduler lifecycle for the whole application.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict

from apscheduler.schedulers.background import BackgroundScheduler


logger = logging.getLogger(__name__)


class AppScheduler:
    """
    Single shared APScheduler instance.

    Jobs can be registered before start, and the scheduler can be stopped and
    started again without losing the registered jobs.
    """

    def __init__(self):
        self._job_specs: Dict[str, Dict[str, Any]] = {}
        self._scheduler: BackgroundScheduler | None = self._build_scheduler()
        self._started = False

    def _build_scheduler(self) -> BackgroundScheduler:
        return BackgroundScheduler(job_defaults={"misfire_grace_time": 300})

    def _restore_jobs(self) -> None:
        if self._scheduler is None:
            self._scheduler = self._build_scheduler()
        for spec in self._job_specs.values():
            if spec["kind"] == "interval":
                self._scheduler.add_job(
                    spec["func"],
                    "interval",
                    minutes=spec["minutes"],
                    id=spec["job_id"],
                    replace_existing=True,
                )
            else:
                self._scheduler.add_job(
                    spec["func"],
                    "cron",
                    id=spec["job_id"],
                    replace_existing=True,
                    **spec["cron_kwargs"],
                )

    def start(self) -> None:
        if self._started:
            return
        if self._scheduler is None:
            self._restore_jobs()
        if self._scheduler is None:
            return
        self._scheduler.start()
        self._started = True
        logger.info("AppScheduler started")

    def stop(self) -> None:
        if not self._started or self._scheduler is None:
            return
        self._scheduler.shutdown(wait=False)
        self._scheduler = None
        self._started = False
        logger.info("AppScheduler stopped")

    def register_interval(self, func: Callable[..., Any], minutes: int, job_id: str) -> None:
        self._job_specs[job_id] = {
            "kind": "interval",
            "func": func,
            "minutes": minutes,
            "job_id": job_id,
        }
        if self._scheduler is None:
            self._restore_jobs()
            return
        self._scheduler.add_job(
            func,
            "interval",
            minutes=minutes,
            id=job_id,
            replace_existing=True,
        )

    def register_cron(self, func: Callable[..., Any], job_id: str, **cron_kwargs: Any) -> None:
        self._job_specs[job_id] = {
            "kind": "cron",
            "func": func,
            "cron_kwargs": cron_kwargs,
            "job_id": job_id,
        }
        if self._scheduler is None:
            self._restore_jobs()
            return
        self._scheduler.add_job(
            func,
            "cron",
            id=job_id,
            replace_existing=True,
            **cron_kwargs,
        )

    def remove_job(self, job_id: str) -> None:
        self._job_specs.pop(job_id, None)
        if self._scheduler is None:
            return
        try:
            self._scheduler.remove_job(job_id)
        except Exception:
            pass


app_scheduler = AppScheduler()
