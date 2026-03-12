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

# путь к конфигу берётся из переменной окружения, иначе ищем рядом с файлом
CONFIG_PATH = os.getenv("CNB_CONFIG", "config.yaml")

# UTC+3 для отметок времени в логах и статусе последней синхронизации
MSK = timezone(timedelta(hours=3))

# время и результат последней синхронизации — хранится в памяти, сбрасывается при рестарте
_last_sync_time: Optional[datetime] = None
_last_sync_rows: Optional[int] = None


def load_config() -> dict:
    """Читает config.yaml и возвращает его содержимое как словарь."""
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def save_config(config: dict) -> None:
    """Перезаписывает config.yaml — вызывается когда пользователь меняет расписание через API."""
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)


# глобальный экземпляр планировщика — создаётся один раз при старте приложения
scheduler = AsyncIOScheduler()


def build_scheduled_job(config: dict):
    """Регистрирует или перерегистрирует задачу ежедневной синхронизации в планировщике.
    Читает расписание и timezone из конфига, строит CronTrigger и добавляет задачу.
    replace_existing=True позволяет вызывать эту функцию повторно при смене расписания
    через API — старая задача заменяется новой без перезапуска контейнера.
    """
    global _last_sync_time, _last_sync_rows
    db_path    = config["database"]["path"]
    currencies = config["currencies"]
    cnb_cfg    = config.get("cnb", {})

    async def _job():
        """Тело задачи планировщика: запускает синхронизацию за сегодня и обновляет статус."""
        global _last_sync_time, _last_sync_rows
        logger.info("Scheduled sync triggered")
        rows = sync_daily(db_path=db_path, currencies=currencies, daily_url=cnb_cfg.get("daily_url"))
        _last_sync_time = datetime.now(MSK)
        _last_sync_rows = rows

    sched_cfg = config.get("scheduler", {})
    tz = config.get("timezone", "UTC")

    if "cron" in sched_cfg:
        parts = sched_cfg["cron"].split()
        # timezone передаём явно — контейнер работает в UTC, без этого время будет неверным
        trigger = CronTrigger(
            minute=parts[0], hour=parts[1],
            day=parts[2], month=parts[3], day_of_week=parts[4],
            timezone=tz,
        )
        logger.info("Scheduler: cron '%s' timezone '%s'", sched_cfg["cron"], tz)
    else:
        seconds = int(sched_cfg.get("interval_seconds", 86400))
        trigger = IntervalTrigger(seconds=seconds)
        logger.info("Scheduler: interval %ds", seconds)

    job = scheduler.add_job(_job, trigger, id="daily_sync", replace_existing=True)
    if scheduler.running:
        logger.info("Next scheduled sync: %s", job.next_run_time)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управляет жизненным циклом приложения.
    Код до yield выполняется при старте: инициализация БД и запуск планировщика.
    Код после yield выполняется при остановке: graceful shutdown планировщика.
    Заменяет устаревшие декораторы @app.on_event('startup') / @app.on_event('shutdown').
    """
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

# монтируем папку static/ — отдаёт JS, CSS и прочие файлы веб-интерфейса
_STATIC = Path(__file__).parent / "static"
if _STATIC.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")


@app.get("/", include_in_schema=False)
async def ui():
    """Отдаёт index.html — точка входа в веб-интерфейс."""
    return FileResponse(str(_STATIC / "index.html"))


def _get_cfg() -> dict:
    """Хелпер: загружает актуальный конфиг при каждом запросе.
    Нужно читать заново, а не кэшировать — конфиг может измениться через /sync/schedule.
    """
    return load_config()


# ---------------------------------------------------------------------------
# Pydantic-модели — описывают структуру входящих и исходящих данных.
# FastAPI автоматически валидирует запросы по этим схемам и генерирует Swagger.
# ---------------------------------------------------------------------------

class SyncDailyRequest(BaseModel):
    """Тело запроса для ручной синхронизации за один день.
    Поле называется sync_date, но в JSON передаётся как 'date' (alias).
    Это сделано потому что имя 'date' конфликтует с импортированным типом date.
    Оба варианта (date и sync_date) принимаются благодаря populate_by_name=True.
    """
    sync_date:  Optional[date_type] = Field(None, alias="date", description="Date YYYY-MM-DD; defaults to today")
    currencies: Optional[list[str]] = Field(None, description="Currencies; defaults to config list")
    model_config = {"populate_by_name": True}


class SyncRangeRequest(BaseModel):
    """Тело запроса для синхронизации за произвольный период.
    start_date и end_date обязательны (Field(...)).
    currencies опциональны — если не переданы, берутся из конфига.
    """
    start_date: date_type           = Field(..., description="Start date YYYY-MM-DD")
    end_date:   date_type           = Field(..., description="End date YYYY-MM-DD")
    currencies: Optional[list[str]] = Field(None, description="Currencies; defaults to config list")


class CurrencyStats(BaseModel):
    """Статистика по одной валюте за период: мин/макс/среднее и количество точек данных."""
    currency:    str
    min_rate:    float
    max_rate:    float
    avg_rate:    float
    data_points: int


class ReportResponse(BaseModel):
    """Ответ эндпоинта /report.
    results — список статистик по валютам у которых есть данные.
    missing — валюты из запроса, для которых данных в БД нет.
    """
    start_date: str
    end_date:   str
    currencies: list[str]
    results:    list[CurrencyStats]
    missing:    list[str] = Field(default_factory=list)


class ScheduleUpdateRequest(BaseModel):
    """Тело запроса для изменения расписания ежедневной синхронизации.
    hour и minute — время запуска. timezone — IANA-название часового пояса.
    """
    hour:     int = Field(..., ge=0, le=23, description="Hour (0-23)")
    minute:   int = Field(..., ge=0, le=59, description="Minute (0-59)")
    timezone: str = Field("UTC", description="IANA timezone, e.g. Europe/Moscow")


# ---------------------------------------------------------------------------
# Эндпоинты
# ---------------------------------------------------------------------------

@app.get("/health", tags=["System"])
async def health():
    """Проверка работоспособности сервиса.
    Используется Docker healthcheck-ом каждые 30 секунд.
    """
    return {"status": "ok"}


@app.get("/sync/status", tags=["Sync"])
async def sync_status():
    """Возвращает информацию о последней синхронизации и текущем расписании.
    Используется веб-интерфейсом для отображения статусной строки.
    last_sync_time и last_sync_rows хранятся в памяти — сбрасываются при рестарте.
    """
    cfg   = _get_cfg()
    cron  = cfg.get("scheduler", {}).get("cron", "1 0 * * *")
    parts = cron.split()
    return {
        "last_sync_time": _last_sync_time.isoformat() if _last_sync_time else None,
        "last_sync_rows": _last_sync_rows,
        "schedule_hour":   int(parts[1]),
        "schedule_minute": int(parts[0]),
        "timezone":        cfg.get("timezone", "UTC"),
    }


@app.post("/sync/schedule", tags=["Sync"])
async def update_schedule(body: ScheduleUpdateRequest):
    """Обновляет расписание ежедневной синхронизации.
    Сохраняет новые значения в config.yaml (чтобы пережили рестарт контейнера),
    затем сразу перепланирует задачу в планировщике без перезапуска приложения.
    """
    cfg = _get_cfg()
    cfg.setdefault("scheduler", {})
    cfg["scheduler"]["cron"] = f"{body.minute} {body.hour} * * *"
    cfg["scheduler"].pop("interval_seconds", None)
    cfg["timezone"] = body.timezone
    save_config(cfg)
    build_scheduled_job(cfg)
    logger.info("Schedule updated: %02d:%02d %s", body.hour, body.minute, body.timezone)
    return {"status": "ok", "hour": body.hour, "minute": body.minute, "timezone": body.timezone}


@app.post("/sync/daily", tags=["Sync"])
async def trigger_daily_sync(body: Optional[SyncDailyRequest] = None):
    """Запускает синхронизацию вручную за один день.
    Если дата не передана — синхронизирует за сегодня.
    Если валюты не переданы — берёт список из конфига.
    Обновляет last_sync_time и last_sync_rows в памяти.
    """
    global _last_sync_time, _last_sync_rows
    cfg        = _get_cfg()
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


@app.post("/sync/range", tags=["Sync"])
async def trigger_range_sync(body: SyncRangeRequest):
    """Запускает синхронизацию за произвольный диапазон дат.
    Проверяет что start_date не позже end_date.
    Логика выбора между year.txt и daily.txt находится в cnb_client.fetch_range.
    """
    if body.start_date > body.end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    cfg        = _get_cfg()
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


@app.get("/report", tags=["Report"], response_model=ReportResponse)
async def get_report_endpoint(
    start_date: Annotated[date_type, Query(description="Start date YYYY-MM-DD")],
    end_date:   Annotated[date_type, Query(description="End date YYYY-MM-DD")],
    currencies: Annotated[list[str], Query(description="e.g. ?currencies=USD&currencies=EUR")],
):
    """Возвращает агрегированный отчёт по курсам за период.
    Параметры передаются как query-строка: /report?start_date=...&end_date=...&currencies=USD
    Для каждой валюты возвращает мин/макс/среднее курса за период.
    Валюты без данных попадают в поле missing, а не вызывают ошибку.
    """
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    cfg     = _get_cfg()
    rows    = get_report(cfg["database"]["path"], str(start_date), str(end_date), currencies)
    found   = {r["currency"] for r in rows}
    missing = [c for c in currencies if c not in found]
    return ReportResponse(
        start_date=str(start_date),
        end_date=str(end_date),
        currencies=currencies,
        results=[CurrencyStats(**r) for r in rows],
        missing=missing,
    )
