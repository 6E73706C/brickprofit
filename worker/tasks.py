import os
from celery import Celery
from celery.schedules import crontab

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

app = Celery("brickprofit", broker=REDIS_URL, backend=REDIS_URL)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_routes={
        "tasks.high_priority_task": {"queue": "high_priority"},
        "tasks.*": {"queue": "default"},
    },
    beat_schedule={
        "cleanup-every-night": {
            "task": "tasks.cleanup",
            "schedule": crontab(hour=2, minute=0),
        },
    },
)


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def send_email(self, to: str, subject: str, body: str):
    """Example async task: send email via external service."""
    try:
        # TODO: integrate with email provider (SES, Mailgun, etc.)
        print(f"Sending email to {to}: {subject}")
    except Exception as exc:
        raise self.retry(exc=exc)


@app.task
def high_priority_task(payload: dict):
    """Fast-lane queue for time-sensitive work."""
    print(f"High priority: {payload}")


@app.task
def cleanup():
    """Scheduled nightly cleanup."""
    print("Running nightly cleanup")
