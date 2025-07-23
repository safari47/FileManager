from celery import Celery
from celery.schedules import crontab

from .config import settings

celery = Celery(
    "src.celery_app",
    broker=settings.CELERY_BROKER,
    backend=settings.CELERY_BACKEND,
    include=["src.tasks.monitor", "src.tasks.download", "src.tasks.upload"],
)

celery.conf.beat_schedule = {
    "scan-all-servers-every-minute": {
        "task": "src.tasks.monitor.scan_all_servers",
        "schedule": crontab(minute="*/1"),
        "options": {
            "queue": "scan_servers",
        },
    }
}
