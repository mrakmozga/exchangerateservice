"""
main.py — FastAPI application with scheduler and Web API.
"""
import logging
import os
from contextlib import asynccontextmanager
from datetime import date as date_type, datetime, timezone, timedelta
from pathlib import Path
from typing import Annotated, Optional

import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from database import init_db, get_report
from sync_service import sync_daily, sync_range

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

CONFIG_PATH = os.getenv("CNB_CONFIG", "config.yaml")

MSK = timezone(timedelta(hours=3))  # Moscow Time, UTC+3

# In-memory state
_last_sync_time: Optional[datetime] = None
_last_sync_rows: Optional[int] = None


def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def save_config(config: dict) -> None:
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)


scheduler = AsyncIOScheduler()


def build_scheduled_job(config: dict):
    global _last_sync_time, _last_sync_rows
    db_path    = config["database"]["path"]
    currencies = config["currencies"]
    cnb_cfg    = config.get("cnb", {})

    async def _job():
        global _last_sync_time, _last_sync_rows
        logger.info("Scheduled sync triggered")
        rows = sync_daily(db_path=db_path, currencies=currencies, daily_url=cnb_cfg.get("daily_url"))
        _last_sync_time = datetime.now(MSK)
        _last_sync_rows = rows

    sched_cfg = config.get("scheduler", {})
    if "cron" in sched_cfg:
        parts = sched_cfg["cron"].split()
        trigger = CronTrigger(
            minute=parts[0], hour=parts[1],
            day=parts[2], month=parts[3], day_of_week=parts[4],
            timezone=MSK,
        )
        logger.info("Scheduler: cron '%s' (Moscow UTC+3)", sched_cfg["cron"])
    else:
        seconds = int(sched_cfg.get("interval_seconds", 86400))
        trigger = IntervalTrigger(seconds=seconds)
        logger.info("Scheduler: interval %ds", seconds)

    job = scheduler.add_job(_job, trigger, id="daily_sync", replace_existing=True)
    if scheduler.running:
        next_run = job.next_run_time
        logger.info("Next scheduled sync: %s", next_run)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = load_config()
    init_db(cfg["database"]["path"])
    build_scheduled_job(cfg)
    scheduler.start()
    logger.info("Application started")
    yield
    scheduler.shutdown(wait=False)
    logger.info("Application stopped")


app = FastAPI(
    title="CNB Exchange Rate Service",
    description="Synchronises Czech National Bank FX rates and exposes a report API.",
    version="1.0.0",
    lifespan=lifespan,
)

_STATIC = Path(__file__).parent / "static"
if _STATIC.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")


@app.get("/", include_in_schema=False)
async def ui():
    return FileResponse(str(_STATIC / "index.html"))


def _get_cfg() -> dict:
    return load_config()


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class SyncDailyRequest(BaseModel):
    sync_date:  Optional[date_type] = Field(None, alias="date", description="Date YYYY-MM-DD; defaults to today")
    currencies: Optional[list[str]] = Field(None, description="Currencies; defaults to config list")

    model_config = {"populate_by_name": True}


class SyncRangeRequest(BaseModel):
    start_date: date_type           = Field(..., description="Start date YYYY-MM-DD")
    end_date:   date_type           = Field(..., description="End date YYYY-MM-DD")
    currencies: Optional[list[str]] = Field(None, description="Currencies; defaults to config list")


class CurrencyStats(BaseModel):
    currency:    str
    min_rate:    float
    max_rate:    float
    avg_rate:    float
    data_points: int


class ReportResponse(BaseModel):
    start_date: str
    end_date:   str
    currencies: list[str]
    results:    list[CurrencyStats]
    missing:    list[str] = Field(default_factory=list)


class ScheduleUpdateRequest(BaseModel):
    hour:   int = Field(..., ge=0, le=23, description="Hour (0-23)")
    minute: int = Field(..., ge=0, le=59, description="Minute (0-59)")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["System"])
async def health():
    return {"status": "ok"}


@app.get("/sync/status", tags=["Sync"], summary="Last sync info")
async def sync_status():
    """Return time and result of the last sync (scheduled or manual)."""
    cfg = _get_cfg()
    sched = cfg.get("scheduler", {})
    cron  = sched.get("cron", "1 0 * * *")
    parts = cron.split()
    return {
        "last_sync_time": _last_sync_time.isoformat() if _last_sync_time else None,
        "last_sync_rows": _last_sync_rows,
        "schedule_hour":   int(parts[1]),
        "schedule_minute": int(parts[0]),

    }


@app.post("/sync/schedule", tags=["Sync"], summary="Update daily sync time")
async def update_schedule(body: ScheduleUpdateRequest):
    """Update the daily auto-sync time and persist it to config.yaml."""
    cfg  = _get_cfg()
    new_cron = f"{body.minute} {body.hour} * * *"
    cfg.setdefault("scheduler", {})
    cfg["scheduler"]["cron"] = new_cron
    cfg["scheduler"].pop("interval_seconds", None)
    save_config(cfg)
    build_scheduled_job(cfg)   # reschedule immediately
    logger.info("Schedule updated to %s", new_cron)
    return {"status": "ok", "cron": new_cron, "hour": body.hour, "minute": body.minute}


@app.post("/sync/daily", tags=["Sync"], summary="Trigger one-day sync")
async def trigger_daily_sync(body: Optional[SyncDailyRequest] = None):
    global _last_sync_time, _last_sync_rows
    cfg = _get_cfg()
    currencies = (body.currencies if body and body.currencies else None) or cfg["currencies"]
    target     = body.sync_date if body else None
    saved = sync_daily(
        db_path=cfg["database"]["path"],
        currencies=currencies,
        target_date=target,
        daily_url=cfg.get("cnb", {}).get("daily_url"),
    )
    _last_sync_time = datetime.now(MSK)
    _last_sync_rows = saved
    return {"status": "ok", "rows_saved": saved, "date": str(target or date_type.today())}


@app.post("/sync/range", tags=["Sync"], summary="Sync a date range")
async def trigger_range_sync(body: SyncRangeRequest):
    if body.start_date > body.end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    cfg = _get_cfg()
    currencies = body.currencies or cfg["currencies"]
    saved = sync_range(
        db_path=cfg["database"]["path"],
        start_date=body.start_date,
        end_date=body.end_date,
        currencies=currencies,
        daily_url=cfg.get("cnb", {}).get("daily_url"),
        year_url=cfg.get("cnb", {}).get("year_url"),
    )
    return {"status": "ok", "rows_saved": saved, "start_date": str(body.start_date), "end_date": str(body.end_date)}


@app.get("/report", tags=["Report"], response_model=ReportResponse, summary="FX rate report for a period")
async def get_report_endpoint(
    start_date: Annotated[date_type, Query(description="Start date YYYY-MM-DD")],
    end_date:   Annotated[date_type, Query(description="End date YYYY-MM-DD")],
    currencies: Annotated[list[str], Query(description="Currency codes, e.g. ?currencies=USD&currencies=EUR")],
):
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    cfg     = _get_cfg()
    db_path = cfg["database"]["path"]
    rows    = get_report(db_path, str(start_date), str(end_date), currencies)
    found   = {r["currency"] for r in rows}
    missing = [c for c in currencies if c not in found]
    return ReportResponse(
        start_date=str(start_date),
        end_date=str(end_date),
        currencies=currencies,
        results=[CurrencyStats(**r) for r in rows],
        missing=missing,
    )
