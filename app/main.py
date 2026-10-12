"""
Employee Attendance & Analytics API

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (a local .env is loaded for convenience; real environment variables win)

Layout of this file:
  1. Configuration and MongoDB client
  2. Time helpers (epoch ms <-> BSON datetime, IST, R1)
  3. Business-rule helpers (R2 - R5)
  4. Request models and validation helpers
  5. Serialisation (MongoDB document -> API shape)
  6. Query / pipeline builders - shared by the endpoints and by /admin/explain
  7. Endpoints
"""
import json
import logging
import math
import os
import re
import threading
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import Decimal128, json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, Strict, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

# --------------------------------------------------------------------------- #
# 1. Configuration and MongoDB client
# --------------------------------------------------------------------------- #
load_dotenv(override=False)  # real environment variables take priority over .env

log = logging.getLogger("attendance")

client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,  # every datetime read back is an aware UTC datetime, never a naive one
    tzinfo=timezone.utc,
    serverSelectionTimeoutMS=3000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]
employees = db["employees"]
logs = db["attendance_logs"]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE = ("PRESENT", "WFH", "ON_DUTY")
NON_PRESENCE = ("ABSENT", "LEAVE")
GRACE_SECONDS = 10 * 60
MIN_OVERTIME_MINUTES = 30
HALF_DAY_HOURS = Decimal("4.50")
MAX_SHIFT = timedelta(hours=24)
MAX_TREND_DAYS = 92

_indexes_ready = False
_index_lock = threading.Lock()


def ensure_indexes() -> None:
    """Idempotent: create_index is a no-op when an identical index already exists."""
    global _indexes_ready
    if _indexes_ready:
        return
    with _index_lock:
        if _indexes_ready:
            return
        # employees ------------------------------------------------------------
        # emp_code is the business key: uniqueness makes concurrent creates race-safe.
        employees.create_index([("emp_code", ASCENDING)], unique=True, name="uniq_emp_code")
        # department filter + headcount cut-off (trend, summary for one department, list by department).
        employees.create_index([("department", ASCENDING), ("joined_on", ASCENDING)], name="department_joined_on")
        # headcount cut-off for the all-department summary.
        employees.create_index([("joined_on", ASCENDING)], name="joined_on")
        # attendance_logs -------------------------------------------------------
        # One record per (emp_code, date): makes punch-in race-safe, and serves per-employee lookups
        # (punch-out, PATCH, monthly, $lookup from employees in summary/trend, list filtered by emp_code).
        logs.create_index([("emp_code", ASCENDING), ("date", ASCENDING)], unique=True, name="uniq_emp_code_date")
        # Date-range scans across all employees (leaderboard) and the default list order.
        logs.create_index([("date", DESCENDING), ("emp_code", ASCENDING)], name="date_desc_emp_code")
        _indexes_ready = True


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        ensure_indexes()
    except PyMongoError as exc:  # keep starting; /health reports 503 and retries index creation
        log.warning("index creation deferred, MongoDB not reachable: %s", exc)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# 2. Time helpers
# --------------------------------------------------------------------------- #
def ms_to_dt(ms: int) -> datetime:
    """Epoch milliseconds -> aware UTC datetime, truncated to whole seconds (R1)."""
    return EPOCH + timedelta(seconds=ms // 1000)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is None:  # defensive: BSON dates are always UTC
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def now_utc() -> datetime:
    """Current instant truncated to whole seconds (R1), for punches."""
    return datetime.now(UTC).replace(microsecond=0)


def now_ms() -> datetime:
    """Current instant at millisecond precision (what BSON stores), for created_at and history."""
    now = datetime.now(UTC)
    return now.replace(microsecond=now.microsecond // 1000 * 1000)


def parse_hhmm(value: str) -> time:
    h, m = map(int, value.split(":"))
    return time(h, m)


def is_overnight(emp: dict) -> bool:
    return parse_hhmm(emp["shift_end"]) <= parse_hhmm(emp["shift_start"])


def attendance_date(punch_in: datetime, emp: dict) -> date:
    """R1: IST calendar date of the punch-in; an early-morning punch on an overnight shift belongs to yesterday."""
    local = punch_in.astimezone(IST)
    day = local.date()
    if is_overnight(emp) and local.time() < parse_hhmm(emp["shift_end"]):
        day -= timedelta(days=1)
    return day


def shift_start_at(day: date, emp: dict) -> datetime:
    return datetime.combine(day, parse_hhmm(emp["shift_start"]), IST)


def shift_end_at(day: date, emp: dict) -> datetime:
    end = datetime.combine(day, parse_hhmm(emp["shift_end"]), IST)
    return end + timedelta(days=1) if is_overnight(emp) else end


def month_bounds(month: str) -> tuple[date, date]:
    y, m = map(int, month.split("-"))
    first = date(y, m, 1)
    nxt = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
    return first, nxt - timedelta(days=1)


def count_weekdays(start: date, end: date) -> int:
    if end < start:
        return 0
    days = (end - start).days + 1
    full_weeks, rest = divmod(days, 7)
    count = full_weeks * 5
    for i in range(rest):
        if (start + timedelta(days=full_weeks * 7 + i)).weekday() < 5:
            count += 1
    return count


def working_days_for(emp: dict, month: str) -> int:
    """R7: Mon-Fri in the month, counted from joined_on for mid-month joiners."""
    first, last = month_bounds(month)
    return count_weekdays(max(first, date.fromisoformat(emp["joined_on"])), last)


def floor_seconds(delta: timedelta) -> int:
    return math.floor(delta.total_seconds())


# --------------------------------------------------------------------------- #
# 3. Business-rule helpers (R2 - R5)
# --------------------------------------------------------------------------- #
def compute_late_minutes(punch_in: datetime, day: date, emp: dict) -> int:
    """R2: late only if strictly more than 10:00 after shift start; then whole minutes since shift start."""
    elapsed = floor_seconds(punch_in - shift_start_at(day, emp))
    return elapsed // 60 if elapsed > GRACE_SECONDS else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600, 2 decimals half-up, computed in Decimal to avoid float artefacts."""
    hours = Decimal(floor_seconds(punch_out - punch_in)) / Decimal(3600)
    return float(hours.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_overtime(punch_out: datetime, day: date, emp: dict) -> int:
    """R3: whole minutes after shift end, only if >= 30 (overnight shifts end on the next day)."""
    minutes = floor_seconds(punch_out - shift_end_at(day, emp)) // 60
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def derived_fields(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime], day: date, emp: dict) -> dict:
    """All four derived fields for a record, per the table in DATA_MODEL.md section 3."""
    if status not in PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    out = {
        "work_hours": None,
        "late_minutes": compute_late_minutes(punch_in, day, emp),
        "overtime_minutes": 0,
        "half_day": False,
    }
    if punch_out is not None:
        wh = compute_work_hours(punch_in, punch_out)
        out["work_hours"] = wh
        out["overtime_minutes"] = compute_overtime(punch_out, day, emp)
        out["half_day"] = Decimal(str(wh)) < HALF_DAY_HOURS  # R5 uses the rounded value
    return out


# --------------------------------------------------------------------------- #
# 4. Request models and validation helpers
# --------------------------------------------------------------------------- #
# Strict: rejects floats, strings and booleans. Bounds reject seconds-looking and out-of-range values.
EpochMillis = Annotated[int, Strict(), Field(ge=100_000_000_000, le=4_102_444_800_000)]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
HHMM = r"^([01]\d|2[0-3]):[0-5]\d$"
MONTH = r"^\d{4}-(0[1-9]|1[0-2])$"


def to_date(value: str) -> date:
    """Strict YYYY-MM-DD (date.fromisoformat alone also accepts e.g. 20260706 or 2026-W27-1)."""
    if not isinstance(value, str) or not DATE_RE.match(value):
        raise ValueError("must be a date in YYYY-MM-DD format")
    return date.fromisoformat(value)


def unprocessable(loc: list, msg: str):
    """422 with the same body shape FastAPI uses for validation errors."""
    raise HTTPException(status_code=422, detail=[{"type": "value_error", "loc": loc, "msg": msg}])


def query_date(value: Optional[str], name: str, where: str = "query") -> Optional[date]:
    if value is None:
        return None
    try:
        return to_date(value)
    except ValueError as exc:
        unprocessable([where, name], str(exc))


def require(params: dict) -> None:
    missing = [k for k, v in params.items() if v is None]
    if missing:
        raise HTTPException(
            status_code=422,
            detail=[{"type": "missing", "loc": ["query", k], "msg": "Field required"} for k in missing],
        )


class EmployeeCreate(BaseModel):
    emp_code: str = Field(pattern=r"^EMP\d{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=HHMM)
    shift_end: str = Field(default="18:30", pattern=HHMM)
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _joined_on(cls, v: str) -> str:
        to_date(v)
        return v

    @model_validator(mode="after")
    def _shift(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMillis] = None
    status: PresenceStatus = "PRESENT"


class PunchOutRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMillis] = None


class RegularizeRequest(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMillis] = None
    punch_out: Optional[EpochMillis] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def _no_explicit_nulls(self):
        # None means "omitted"; an explicit null would be ambiguous (the schema does not allow it).
        for name in ("status", "punch_in", "punch_out"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} may not be null; omit it to leave it unchanged")
        return self


Page = Annotated[int, Query(ge=1)]
PageSize = Annotated[int, Query(ge=1, le=100)]
Month = Annotated[str, Query(pattern=MONTH)]


# --------------------------------------------------------------------------- #
# 5. Serialisation
# --------------------------------------------------------------------------- #
def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start"),
        "shift_end": doc.get("shift_end"),
        "joined_on": doc.get("joined_on"),
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def _change_value(v: Any) -> Any:
    return dt_to_ms(v) if isinstance(v, datetime) else v


def record_out(doc: dict) -> dict:
    history = [
        {
            "at": dt_to_ms(h.get("at")),
            "by": h.get("by"),
            "reason": h.get("reason"),
            "changes": {
                field: {"from": _change_value(c.get("from")), "to": _change_value(c.get("to"))}
                for field, c in (h.get("changes") or {}).items()
            },
        }
        for h in (doc.get("history") or [])
    ]
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day") or False),
        "history": history,
    }


def get_employee(emp_code: str) -> dict:
    emp = employees.find_one({"emp_code": emp_code}, {"_id": 0})
    if emp is None:
        raise HTTPException(status_code=404, detail="employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 6. Query / pipeline builders (one source of truth for endpoint and explain)
# --------------------------------------------------------------------------- #
def _dec(value: str) -> Decimal128:
    return Decimal128(value)


def round_half_up(expr: Any, places: int) -> dict:
    """R8 inside the pipeline: Decimal128 arithmetic, floor(x * 10^p + 0.5) / 10^p, back to double.
    ($round is banker's rounding, and doing it on doubles reintroduces binary artefacts.)"""
    scale = 10 ** places
    return {
        "$toDouble": {
            "$divide": [
                {"$floor": {"$add": [{"$multiply": [{"$toDecimal": expr}, scale]}, _dec("0.5")]}},
                scale,
            ]
        }
    }


def is_presence(p: str) -> dict:
    return {"$in": [f"{p}status", list(PRESENCE)]}


def is_weekday(date_field: str) -> dict:
    return {"$lte": [{"$isoDayOfWeek": {"$dateFromString": {"dateString": date_field, "format": "%Y-%m-%d"}}}, 5]}


def present_value(p: str, weekdays_only: bool) -> dict:
    """R5/R6/R7: 1 for a presence record, 0.5 for a half day, 0 otherwise; optionally Mon-Fri only."""
    cond = is_presence(p)
    if weekdays_only:
        cond = {"$and": [cond, is_weekday(f"{p}date")]}
    return {"$cond": [cond, {"$cond": [{"$ifNull": [f"{p}half_day", False]}, 0.5, 1]}, 0]}


def late_of(p: str) -> dict:
    return {"$ifNull": [f"{p}late_minutes", 0]}


def is_late(p: str) -> dict:
    return {"$gt": [late_of(p), 0]}


def build_attendance_list(emp_code, date_from, date_to, status, page, page_size) -> dict:
    q: dict = {}
    if emp_code is not None:
        q["emp_code"] = emp_code
    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from.isoformat()
        if date_to:
            q["date"]["$lte"] = date_to.isoformat()
    if status is not None:
        q["status"] = status
    return {
        "filter": q,
        "sort": [("date", DESCENDING), ("emp_code", ASCENDING)],
        "skip": (page - 1) * page_size,
        "limit": page_size,
    }


def build_employee_monthly(emp_code: str, month: str, working_days: int) -> list:
    """working_days is computed from the calendar and joined_on (R7) and passed in as a constant."""
    first, last = month_bounds(month)
    pct: Any = {"$literal": None}
    if working_days > 0:
        pct = round_half_up({"$divide": [{"$multiply": [{"$toDecimal": "$present_days"}, 100]}, working_days]}, 2)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
        {
            "$group": {
                "_id": None,
                "present_days": {"$sum": present_value("$", weekdays_only=True)},
                "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                "late_count": {"$sum": {"$cond": [is_late("$"), 1, 0]}},
                "total_late_minutes": {"$sum": late_of("$")},
                "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "present_days": "$present_days",
                "leave_days": "$leave_days",
                "late_count": "$late_count",
                "total_late_minutes": "$total_late_minutes",
                "total_overtime_minutes": "$total_overtime_minutes",
                "attendance_pct": pct,
            }
        },
    ]


def build_department_summary(month: str, department: Optional[str]) -> list:
    """Driven from employees so that people with zero logs are still counted (R9)."""
    first, last = month_bounds(month)
    emp_match: dict = {"joined_on": {"$lte": last.isoformat()}}
    if department is not None:
        emp_match["department"] = department
    has_work_hours = {"$and": [is_presence("$$l."), {"$isNumber": "$$l.work_hours"}]}

    def per_log_sum(expr: Any) -> dict:
        return {"$sum": {"$map": {"input": "$logs", "as": "l", "in": expr}}}

    return [
        {"$match": emp_match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
                    {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "work_hours": 1, "late_minutes": 1}},
                ],
                "as": "logs",
            }
        },
        {
            "$project": {
                "_id": 0,
                "department": 1,
                "present": per_log_sum(present_value("$$l.", weekdays_only=True)),
                # Work hours are summed as Decimal128 so the mean is exact before half-up rounding.
                "wh_sum": per_log_sum({"$cond": [has_work_hours, {"$toDecimal": "$$l.work_hours"}, _dec("0")]}),
                "wh_count": per_log_sum({"$cond": [has_work_hours, 1, 0]}),
                "late_count": per_log_sum({"$cond": [is_late("$$l."), 1, 0]}),
                "late_sum": per_log_sum(late_of("$$l.")),
                "leave_count": per_log_sum({"$cond": [{"$eq": ["$$l.status", "LEAVE"]}, 1, 0]}),
                "on_duty_count": per_log_sum({"$cond": [{"$eq": ["$$l.status", "ON_DUTY"]}, 1, 0]}),
            }
        },
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_days": {"$sum": "$present"},
                "wh_sum": {"$sum": "$wh_sum"},
                "wh_count": {"$sum": "$wh_count"},
                "late_count": {"$sum": "$late_count"},
                "total_late_minutes": {"$sum": "$late_sum"},
                "leave_count": {"$sum": "$leave_count"},
                "on_duty_count": {"$sum": "$on_duty_count"},
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "_id": 0,
                "department": "$_id",
                "headcount": "$headcount",
                "present_days": {"$toDouble": "$present_days"},
                "avg_work_hours": {
                    "$cond": [
                        {"$gt": ["$wh_count", 0]},
                        round_half_up({"$divide": ["$wh_sum", "$wh_count"]}, 2),
                        None,
                    ]
                },
                "late_count": "$late_count",
                "total_late_minutes": "$total_late_minutes",
                "leave_count": "$leave_count",
                "on_duty_count": "$on_duty_count",
            }
        },
    ]


def build_late_leaderboard(month: str, limit: int, department: Optional[str]) -> list:
    first, last = month_bounds(month)
    pipeline: list = [
        {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {
            "$lookup": {
                "from": "employees",
                "localField": "_id",
                "foreignField": "emp_code",
                "pipeline": [{"$project": {"_id": 0, "name": 1, "department": 1}}],
                "as": "emp",
            }
        },
        {"$unwind": "$emp"},  # drops logs whose employee does not exist
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank inside the department only
    pipeline += [
        # Standard competition ranking (1, 2, 2, 4): $rank, not the array position.
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies after ranking, so ties at the cutoff all stay
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": "$rank",
                "emp_code": "$_id",
                "name": "$emp.name",
                "department": "$emp.department",
                "total_late_minutes": "$total_late_minutes",
                "late_count": "$late_count",
            }
        },
    ]
    return pipeline


def build_department_trend(department: str, start: date, end: date) -> list:
    """One row per day: per-employee events -> group by day -> $densify gaps -> running headcount -> windows."""
    from_s, to_s = start.isoformat(), end.isoformat()
    from_dt = datetime.combine(start, time(), UTC)
    to_excl_dt = datetime.combine(end + timedelta(days=1), time(), UTC)
    rate_dec = {"$divide": [{"$toDecimal": "$present_count"}, "$headcount"]}
    return [
        {"$match": {"department": department, "joined_on": {"$lte": to_s}}},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": from_s, "$lte": to_s}}},
                    {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "late_minutes": 1}},
                ],
                "as": "logs",
            }
        },
        {
            "$project": {
                "_id": 0,
                "events": {
                    "$concatArrays": [
                        # +1 headcount from the day the employee counts (joined_on, or `from` if earlier).
                        [{"date": {"$max": ["$joined_on", from_s]}, "present": 0, "late": 0, "hc": 1}],
                        {
                            "$map": {
                                "input": "$logs",
                                "as": "l",
                                "in": {
                                    "date": "$$l.date",
                                    "present": present_value("$$l.", weekdays_only=False),
                                    "late": {"$cond": [is_late("$$l."), 1, 0]},
                                    "hc": 0,
                                },
                            }
                        },
                    ]
                },
            }
        },
        {"$unwind": "$events"},
        {"$replaceWith": "$events"},
        # Guarantees at least one input document so $densify always produces the full range.
        {"$unionWith": {"pipeline": [{"$documents": [{"date": from_s, "present": 0, "late": 0, "hc": 0}]}]}},
        {"$group": {"_id": "$date", "present": {"$sum": "$present"}, "late": {"$sum": "$late"}, "hc": {"$sum": "$hc"}}},
        {"$set": {"day": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d", "timezone": "UTC"}}}},
        {"$densify": {"field": "day", "range": {"step": 1, "unit": "day", "bounds": [from_dt, to_excl_dt]}}},
        {
            "$setWindowFields": {
                "sortBy": {"day": 1},
                "output": {
                    "headcount": {"$sum": {"$ifNull": ["$hc", 0]}, "window": {"documents": ["unbounded", "current"]}}
                },
            }
        },
        {
            "$set": {
                "present_count": {"$ifNull": ["$present", 0]},
                "late_count": {"$ifNull": ["$late", 0]},
                "is_working_day": {"$lte": [{"$isoDayOfWeek": "$day"}, 5]},
            }
        },
        {
            "$set": {
                # Kept as Decimal128 (rounded half-up to 4 places) so the moving average is exact too.
                "rate": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        {"$toDecimal": round_half_up(rate_dec, 4)},
                        None,
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"day": 1},
                # $avg skips nulls and returns null when the window has no numeric value.
                "output": {"ma": {"$avg": "$rate", "window": {"documents": [-6, 0]}}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": {"$dateToString": {"date": "$day", "format": "%Y-%m-%d"}},
                "is_working_day": "$is_working_day",
                "headcount": "$headcount",
                "present_count": {"$toDouble": "$present_count"},
                "late_count": "$late_count",
                "attendance_rate": {"$toDouble": "$rate"},
                "moving_avg_7d": round_half_up("$ma", 4),
            }
        },
    ]


def validate_trend_range(start: date, end: date) -> None:
    if end < start:
        unprocessable(["query", "to"], "`to` must not be before `from`")
    if (end - start).days + 1 > MAX_TREND_DAYS:
        unprocessable(["query", "to"], f"range may not exceed {MAX_TREND_DAYS} days")


# --------------------------------------------------------------------------- #
# 7. Endpoints
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
        ensure_indexes()
    except PyMongoError:
        raise HTTPException(status_code=503, detail="MongoDB is not reachable")
    return {"status": "ok"}


# ---- employees -------------------------------------------------------------
@app.post("/employees", status_code=201)
def create_employee(body: EmployeeCreate):
    doc = body.model_dump()
    doc["created_at"] = now_ms()
    try:
        employees.insert_one(doc)  # the unique index decides; no check-then-insert race
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(department: Optional[str] = None, page: Page = 1, page_size: PageSize = 20):
    q = {"department": department} if department is not None else {}
    total = employees.count_documents(q)
    cursor = employees.find(q, {"_id": 0}).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


# ---- attendance ------------------------------------------------------------
@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInRequest):
    emp = get_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    day = attendance_date(ts, emp)
    doc = {
        "emp_code": body.emp_code,
        "date": day.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derived_fields(body.status, ts, None, day, emp),
        "history": [],
    }
    try:
        logs.insert_one(doc)  # unique (emp_code, date): concurrent duplicates fail here, atomically
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutRequest):
    emp = get_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    # Most recent record whose punch_in <= ts. Dates are monotonic with punch_in (one record per date),
    # so sorting by date walks the (emp_code, date) index backwards; works across midnight.
    rec = logs.find_one({"emp_code": body.emp_code, "punch_in": {"$ne": None, "$lte": ts}}, sort=[("date", DESCENDING)])
    if rec is None:
        raise HTTPException(status_code=404, detail="no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(status_code=409, detail="already punched out")
    pin = rec["punch_in"]
    if ts <= pin:
        unprocessable(["body", "punched_at"], "punch-out must be after punch-in")
    if ts - pin > MAX_SHIFT:
        unprocessable(["body", "punched_at"], "punch-out must be within 24 hours of punch-in")
    day = date.fromisoformat(rec["date"])
    derived = derived_fields(rec["status"], pin, ts, day, emp)
    # Conditional update: of two simultaneous punch-outs only one still sees punch_out == null.
    updated = logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None},
        {"$set": {"punch_out": ts, **derived}},
        projection={"_id": 0},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(status_code=409, detail="already punched out")
    return record_out(updated)


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    d_from, d_to = query_date(date_from, "date_from"), query_date(date_to, "date_to")
    if d_from and d_to and d_from > d_to:
        unprocessable(["query", "date_from"], "date_from must not be after date_to")
    spec = build_attendance_list(emp_code, d_from, d_to, status, page, page_size)
    total = logs.count_documents(spec["filter"]) if spec["filter"] else logs.estimated_document_count()
    cursor = logs.find(spec["filter"], {"_id": 0}).sort(spec["sort"]).skip(spec["skip"]).limit(spec["limit"])
    return {"items": [record_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


TRACKED_FIELDS = ("status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day")
PATCH_RETRIES = 5


def _current_values(doc: dict) -> dict:
    """Stored values with the documented defaults for missing fields."""
    return {
        "status": doc["status"],
        "punch_in": doc.get("punch_in"),
        "punch_out": doc.get("punch_out"),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day") or False),
    }


def _same(a: Any, b: Any) -> bool:
    if isinstance(a, datetime) or isinstance(b, datetime):
        return dt_to_ms(a) == dt_to_ms(b)
    return a == b


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeRequest):  # noqa: A002 - name fixed by the contract
    day = query_date(date, "date", where="path")
    emp = get_employee(emp_code)
    new_pin = ms_to_dt(body.punch_in) if body.punch_in is not None else None
    new_pout = ms_to_dt(body.punch_out) if body.punch_out is not None else None

    for _ in range(PATCH_RETRIES):
        doc = logs.find_one({"emp_code": emp_code, "date": day.isoformat()})
        if doc is None:
            raise HTTPException(status_code=404, detail="no attendance record for that date")
        old = _current_values(doc)
        status = body.status or old["status"]

        if status in NON_PRESENCE:
            if body.punch_in is not None or body.punch_out is not None:
                unprocessable(["body"], f"status {status} cannot have punch times")
            pin, pout = None, None
        else:
            pin = new_pin if new_pin is not None else old["punch_in"]
            pout = new_pout if new_pout is not None else old["punch_out"]
            if pin is None:
                unprocessable(["body", "punch_in"], f"status {status} requires a punch_in")
            if new_pin is not None and attendance_date(pin, emp) != day:
                unprocessable(["body", "punch_in"], "punch_in must stay on the record's attendance date")
            if pout is not None:
                if pout <= pin:
                    unprocessable(["body", "punch_out"], "punch_out must be after punch_in")
                if pout - pin > MAX_SHIFT:
                    unprocessable(["body", "punch_out"], "punch_out must be within 24 hours of punch_in")

        new = {"status": status, "punch_in": pin, "punch_out": pout, **derived_fields(status, pin, pout, day, emp)}
        changes = {f: {"from": old[f], "to": new[f]} for f in TRACKED_FIELDS if not _same(old[f], new[f])}
        if not changes:
            unprocessable(["body"], "the request does not change anything")

        entry = {"at": now_ms(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
        # Optimistic concurrency: only apply if nobody changed the record (or its history) since we read it.
        guard = {
            "_id": doc["_id"],
            "status": doc["status"],
            "punch_in": doc.get("punch_in"),
            "punch_out": doc.get("punch_out"),
            "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, len(doc.get("history") or [])]},
        }
        updated = logs.find_one_and_update(
            guard,
            {"$set": new, "$push": {"history": entry}},
            projection={"_id": 0},
            return_document=ReturnDocument.AFTER,
        )
        if updated is not None:
            return record_out(updated)
        # Lost the race: re-read and re-apply on top of the other edit, so neither history entry is lost.
    raise HTTPException(status_code=409, detail="record was modified concurrently, please retry")


# ---- analytics -------------------------------------------------------------
@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: Month):
    emp = get_employee(emp_code)
    working_days = working_days_for(emp, month)
    rows = list(logs.aggregate(build_employee_monthly(emp_code, month, working_days)))
    # No records in the month -> the $group emits nothing; every count is then 0.
    agg = rows[0] if rows else {"attendance_pct": 0.0 if working_days else None}
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": float(agg.get("present_days", 0)),
        "leave_days": int(agg.get("leave_days", 0)),
        "late_count": int(agg.get("late_count", 0)),
        "total_late_minutes": int(agg.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(agg.get("total_overtime_minutes", 0)),
        "attendance_pct": agg["attendance_pct"],
    }


@app.get("/analytics/departments/summary")
def department_summary(month: Month, department: Optional[str] = None):
    items = list(employees.aggregate(build_department_summary(month, department)))
    for it in items:
        for key in ("headcount", "late_count", "total_late_minutes", "leave_count", "on_duty_count"):
            it[key] = int(it[key])
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: Month, limit: Annotated[int, Query(ge=1, le=50)] = 10, department: Optional[str] = None):
    items = list(logs.aggregate(build_late_leaderboard(month, limit, department)))
    for it in items:
        it["total_late_minutes"] = int(it["total_late_minutes"])
    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_: Annotated[str, Query(alias="from")],
    to: str,
):
    start, end = query_date(from_, "from"), query_date(to, "to")
    validate_trend_range(start, end)
    if employees.find_one({"department": department}, {"_id": 1}) is None:
        raise HTTPException(status_code=404, detail="unknown department")
    items = list(employees.aggregate(build_department_trend(department, start, end)))
    return {"department": department, "items": items}


# ---- admin -----------------------------------------------------------------
ExplainTarget = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"]


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: ExplainTarget,
    emp_code: Optional[str] = None,
    month: Annotated[Optional[str], Query(pattern=MONTH)] = None,
    department: Optional[str] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Annotated[Optional[str], Query(alias="from")] = None,
    to: Optional[str] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    if endpoint == "attendance_list":
        d_from, d_to = query_date(date_from, "date_from"), query_date(date_to, "date_to")
        if d_from and d_to and d_from > d_to:
            unprocessable(["query", "date_from"], "date_from must not be after date_to")
        spec = build_attendance_list(emp_code, d_from, d_to, status, page, page_size)
        collection = logs.name
        command = {
            "find": collection,
            "filter": spec["filter"],
            "projection": {"_id": 0},
            "sort": dict(spec["sort"]),
            "skip": spec["skip"],
            "limit": spec["limit"],
        }
    else:
        if endpoint == "employee_monthly":
            require({"emp_code": emp_code, "month": month})
            emp = employees.find_one({"emp_code": emp_code}, {"_id": 0, "joined_on": 1})
            working_days = working_days_for(emp, month) if emp else 0
            collection, pipeline = logs.name, build_employee_monthly(emp_code, month, working_days)
        elif endpoint == "department_summary":
            require({"month": month})
            collection, pipeline = employees.name, build_department_summary(month, department)
        elif endpoint == "late_leaderboard":
            require({"month": month})
            collection, pipeline = logs.name, build_late_leaderboard(month, limit, department)
        else:  # department_trend
            require({"department": department, "from": from_, "to": to})
            start, end = query_date(from_, "from"), query_date(to, "to")
            validate_trend_range(start, end)
            collection, pipeline = employees.name, build_department_trend(department, start, end)
        command = {"aggregate": collection, "pipeline": pipeline, "cursor": {}}

    raw = db.command("explain", command, verbosity="executionStats")
    return {"endpoint": endpoint, "collection": collection, "explain": json.loads(json_util.dumps(raw))}
