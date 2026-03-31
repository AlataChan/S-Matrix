from app_scheduler import AppScheduler
from datasource_handler import SyncScheduler


def test_app_scheduler_idempotent_start():
    scheduler = AppScheduler()
    scheduler.register_interval(lambda: None, minutes=1, job_id="job-1")

    scheduler.start()
    scheduler.start()

    assert sorted(job.id for job in scheduler._scheduler.get_jobs()) == ["job-1"]

    scheduler.stop()


def test_app_scheduler_register_before_start():
    scheduler = AppScheduler()

    scheduler.register_cron(lambda: None, job_id="job-2", hour=0, minute=0)

    assert sorted(job.id for job in scheduler._scheduler.get_jobs()) == ["job-2"]


def test_app_scheduler_stop_is_clean():
    scheduler = AppScheduler()
    scheduler.register_interval(lambda: None, minutes=1, job_id="job-3")

    scheduler.start()
    scheduler.stop()
    scheduler.start()

    assert sorted(job.id for job in scheduler._scheduler.get_jobs()) == ["job-3"]

    scheduler.stop()


def test_sync_scheduler_registers_expected_jobs():
    shared_scheduler = AppScheduler()
    sync_scheduler = SyncScheduler(handler=object())

    sync_scheduler.register(shared_scheduler)

    assert sorted(job.id for job in shared_scheduler._scheduler.get_jobs()) == [
        "field_catalog_refresh",
        "sync_checker",
    ]
