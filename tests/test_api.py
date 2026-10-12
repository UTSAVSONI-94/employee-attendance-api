"""
End-to-end checks for app/main.py against a real MongoDB.

    python tests/test_api.py

Starts the app in-process on port 8765 against the database in MONGO_TEST_DB (default "attendance_test"),
which is DROPPED and re-seeded first. Needs httpx (pip install httpx). Prints one line per check.
"""
import os
import sys
import threading
import time as _time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

os.environ["MONGO_DB"] = os.getenv("MONGO_TEST_DB", "attendance_test")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx  # noqa: E402
import uvicorn  # noqa: E402

from app import main  # noqa: E402

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
PORT = 8765
BASE = f"http://127.0.0.1:{PORT}"
failures: list[str] = []


def ist_ms(y, mo, d, h, mi, s=0, ms=0) -> int:
    return (datetime(y, mo, d, h, mi, s, ms * 1000, tzinfo=IST) - EPOCH) // timedelta(milliseconds=1)


def ist_dt(*a) -> datetime:
    return datetime(*a, tzinfo=IST).astimezone(UTC)


def check(name: str, cond: bool, detail: object = "") -> None:
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  -> {detail}"))
    if not cond:
        failures.append(name)


# --------------------------------------------------------------------------- #
# Seed data
# --------------------------------------------------------------------------- #
def emp(code, dept, joined, start="09:30", end="18:30"):
    return {"emp_code": code, "name": f"Name {code}", "email": f"{code.lower()}@example.com", "department": dept,
            "shift_start": start, "shift_end": end, "joined_on": joined, "created_at": datetime(2026, 1, 1, tzinfo=UTC)}


def log(code, day, status="PRESENT", wh=None, late=0, ot=0, half=False, legacy=False):
    y, m, d = map(int, day.split("-"))
    doc = {"emp_code": code, "date": day, "status": status,
           "punch_in": ist_dt(y, m, d, 9, 30) if status in main.PRESENCE else None,
           "punch_out": ist_dt(y, m, d, 18, 30) if wh is not None else None,
           "work_hours": wh, "late_minutes": late, "overtime_minutes": ot, "half_day": half, "history": []}
    if legacy:
        for k in ("half_day", "history", "late_minutes"):
            doc.pop(k)
    return doc


def seed():
    main.client.drop_database(main.db.name)
    main.employees.insert_many([
        emp("EMP1001", "QA", "2026-01-01"), emp("EMP1002", "QA", "2026-01-01"),
        emp("EMP1003", "QA", "2026-08-17"),            # mid-month joiner
        emp("EMP1004", "QA", "2026-09-05"),            # joins after August: not in August headcount
        emp("EMP1005", "QA", "2026-01-01"),            # zero logs: still in headcount
        emp("EMP2001", "Ops", "2026-01-01"), emp("EMP2002", "Ops", "2026-01-01"), emp("EMP2003", "Ops", "2026-01-01"),
        emp("EMP2004", "Ops", "2026-08-20"),
        emp("EMP3001", "Future", "2027-01-01"),
        # punch tests
        emp("EMP5001", "Punch", "2026-01-01"),
        emp("EMP5002", "Punch", "2026-01-01", "22:00", "06:00"),  # overnight
        emp("EMP5003", "Punch", "2026-01-01"), emp("EMP5004", "Punch", "2026-01-01"),
    ])
    main.logs.insert_many([
        log("EMP1001", "2026-08-03", wh=9.0), log("EMP1001", "2026-08-04", wh=8.0, late=20),
        log("EMP1001", "2026-08-01", wh=4.0, half=True, late=15, ot=60),       # Saturday
        log("EMP1001", "2026-08-05", "WFH", wh=4.0, half=True), log("EMP1001", "2026-08-06", "LEAVE"),
        log("EMP1001", "2026-08-07", "ON_DUTY", wh=8.5),
        log("EMP1002", "2026-08-03", wh=7.0, late=100), log("EMP1002", "2026-08-04", "ABSENT"),
        log("EMP1002", "2026-08-10", wh=8.25, legacy=True),
        log("EMP1003", "2026-08-17", wh=8.0), log("EMP1003", "2026-08-18", late=50),  # open record
        log("EMP2001", "2026-08-03", wh=8.01, late=120), log("EMP2002", "2026-08-03", wh=8.00, late=60),
        log("EMP2002", "2026-08-04", late=40), log("EMP2003", "2026-08-03", late=50),
        log("GHOST01", "2026-08-03", wh=8.0, late=500),                         # orphan: ignored
    ])


# --------------------------------------------------------------------------- #
# Checks
# --------------------------------------------------------------------------- #
def test_employees(c: httpx.Client):
    body = {"emp_code": "EMP9001", "name": "New", "email": "new@example.com", "department": "Temp", "joined_on": "2026-08-01"}
    with ThreadPoolExecutor(8) as ex:
        codes = sorted(r.status_code for r in ex.map(lambda _: c.post("/employees", json=body), range(8)))
    check("create employee race: one 201, rest 409", codes == [201] + [409] * 7, codes)
    r = c.get("/employees", params={"department": "QA", "page": 1, "page_size": 2})
    j = r.json()
    check("list employees: filtered total", j["total"] == 5, j)
    check("list employees: sorted by emp_code", [e["emp_code"] for e in j["items"]] == ["EMP1001", "EMP1002"], j)
    check("list employees: created_at epoch ms", isinstance(j["items"][0]["created_at"], int))
    j2 = c.get("/employees", params={"department": "QA", "page": 2, "page_size": 2}).json()
    check("list employees: page 2 starts at 3rd", j2["items"][0]["emp_code"] == "EMP1003", j2)
    for bad, name in [({**body, "emp_code": "X1"}, "bad emp_code"), ({**body, "email": "nope"}, "bad email"),
                      ({**body, "emp_code": "EMP9002", "shift_start": "09:00", "shift_end": "09:00"}, "equal shifts"),
                      ({**body, "emp_code": "EMP9003", "joined_on": "20260801"}, "bad joined_on"),
                      ({**body, "emp_code": "EMP9004", "shift_start": "24:00"}, "bad shift")]:
        check(f"create employee 422: {name}", c.post("/employees", json=bad).status_code == 422)
    for params in ({"page": 0}, {"page_size": 101}, {"page_size": 0}):
        check(f"list employees 422 {params}", c.get("/employees", params=params).status_code == 422)


def test_punch(c: httpx.Client):
    p = lambda **kw: c.post("/attendance/punch-in", json=kw)
    # R2 examples, each on its own day
    for day, (h, m, s, ms), late in [(7, (9, 40, 0, 900), 0), (8, (9, 40, 1, 0), 10), (9, (10, 15, 59, 0), 45)]:
        r = p(emp_code="EMP5001", punched_at=ist_ms(2026, 9, day, h, m, s, ms))
        j = r.json()
        check(f"R2 late {h}:{m}:{s}.{ms} -> {late}", r.status_code == 201 and j["late_minutes"] == late, j)
    j = c.get("/attendance", params={"emp_code": "EMP5001", "date_from": "2026-09-07", "date_to": "2026-09-07"}).json()
    check("R1 truncation stored and returned", j["items"][0]["punch_in"] == ist_ms(2026, 9, 7, 9, 40, 0), j)
    check("punch-in record shape", set(j["items"][0]) == {"emp_code", "date", "status", "punch_in", "punch_out",
          "work_hours", "late_minutes", "overtime_minutes", "half_day", "history"}, j["items"][0].keys())

    # validation
    for val, name in [(1783312500, "seconds"), (1783312500000.0, "float"), ("1783312500000", "string"), (True, "bool"),
                      (5_000_000_000_000, "out of range")]:
        check(f"punched_at 422: {name}", p(emp_code="EMP5001", punched_at=val).status_code == 422)
    check("punch-in status ABSENT 422", p(emp_code="EMP5001", status="ABSENT").status_code == 422)
    check("punch-in unknown employee 404", p(emp_code="EMP0000", punched_at=ist_ms(2026, 9, 7, 9, 0)).status_code == 404)

    # race
    ts = ist_ms(2026, 9, 14, 9, 0)
    with ThreadPoolExecutor(10) as ex:
        codes = sorted(r.status_code for r in ex.map(lambda _: p(emp_code="EMP5003", punched_at=ts), range(10)))
    check("punch-in race: one 201, rest 409", codes == [201] + [409] * 9, codes)

    # punch-out
    o = lambda **kw: c.post("/attendance/punch-out", json=kw)
    check("punch-out unknown emp 404", o(emp_code="EMP0000", punched_at=ts).status_code == 404)
    check("punch-out without punch-in 404", o(emp_code="EMP5004", punched_at=ts).status_code == 404)
    check("punch-out == punch_in 422", o(emp_code="EMP5003", punched_at=ts).status_code == 422)
    check("punch-out > 24h 422", o(emp_code="EMP5003", punched_at=ts + 24 * 3600_000 + 1000).status_code == 422)
    # 4h29m06s = 16146 s = 4.485 h -> half-up 4.49 (banker's/float would give 4.48); 4.49 < 4.5 -> half day
    out = ts + 16146_000
    with ThreadPoolExecutor(6) as ex:
        res = list(ex.map(lambda _: o(emp_code="EMP5003", punched_at=out), range(6)))
    codes = sorted(r.status_code for r in res)
    check("punch-out race: one 200, rest 409", codes == [200] + [409] * 5, codes)
    j = next(r.json() for r in res if r.status_code == 200)
    check("R4 half-up 4.485 -> 4.49", j["work_hours"] == 4.49, j)
    check("R5 half day", j["half_day"] is True, j)
    check("punch-out history untouched", j["history"] == [], j)

    # R3 threshold: shift end 18:30; 18:59:59 -> 29 min -> 0; 19:00 -> 30
    p(emp_code="EMP5004", punched_at=ist_ms(2026, 9, 15, 9, 30))
    j = o(emp_code="EMP5004", punched_at=ist_ms(2026, 9, 15, 18, 59, 59)).json()
    check("R3 29 min -> 0 overtime", j["overtime_minutes"] == 0 and j["half_day"] is False, j)
    p(emp_code="EMP5004", punched_at=ist_ms(2026, 9, 16, 9, 30))
    j = o(emp_code="EMP5004", punched_at=ist_ms(2026, 9, 16, 19, 0)).json()
    check("R3 30 min -> 30 overtime", j["overtime_minutes"] == 30 and j["work_hours"] == 9.5, j)

    # overnight shift (22:00 - 06:00)
    j = p(emp_code="EMP5002", punched_at=ist_ms(2026, 7, 6, 21, 55)).json()
    check("overnight punch-in 21:55 -> same day, not late", j["date"] == "2026-07-06" and j["late_minutes"] == 0, j)
    j = o(emp_code="EMP5002", punched_at=ist_ms(2026, 7, 7, 6, 40)).json()
    check("overnight punch-out across midnight", j["date"] == "2026-07-06" and j["overtime_minutes"] == 40
          and j["work_hours"] == 8.75, j)
    j = p(emp_code="EMP5002", punched_at=ist_ms(2026, 7, 8, 0, 30)).json()
    check("overnight punch-in 00:30 -> previous day, 150 late", j["date"] == "2026-07-07" and j["late_minutes"] == 150, j)


def test_list_attendance(c: httpx.Client):
    j = c.get("/attendance", params={"date_from": "2026-08-03", "date_to": "2026-08-03", "page_size": 100}).json()
    keys = [(i["date"], i["emp_code"]) for i in j["items"]]
    check("list attendance: filtered total", j["total"] == 6, j["total"])
    check("list attendance: sorted emp_code asc within date", keys == sorted(keys), keys)
    j = c.get("/attendance", params={"date_from": "2026-08-01", "date_to": "2026-08-31", "page_size": 3}).json()
    dates = [i["date"] for i in j["items"]]
    check("list attendance: date desc", dates == sorted(dates, reverse=True), dates)
    j = c.get("/attendance", params={"status": "LEAVE"}).json()
    check("list attendance: status filter", j["total"] == 1 and j["items"][0]["status"] == "LEAVE", j)
    legacy = c.get("/attendance", params={"emp_code": "EMP1002", "date_from": "2026-08-10"}).json()["items"][0]
    check("legacy record defaults", legacy["history"] == [] and legacy["half_day"] is False
          and legacy["late_minutes"] == 0, legacy)
    for params in ({"date_from": "2026-08-05", "date_to": "2026-08-01"}, {"date_from": "2026-13-01"},
                   {"status": "SICK"}, {"page_size": 500}):
        check(f"list attendance 422 {params}", c.get("/attendance", params=params).status_code == 422)


def test_regularize(c: httpx.Client):
    url = "/attendance/EMP5001/2026-09-09"
    before = c.get("/attendance", params={"emp_code": "EMP5001", "date_from": "2026-09-09", "date_to": "2026-09-09"}).json()["items"][0]
    new_in = ist_ms(2026, 9, 9, 9, 28)
    r = c.patch(url, json={"punch_in": new_in, "reason": "biometric glitch", "regularized_by": "hr.admin", "late_minutes": 99})
    j = r.json()
    check("PATCH punch_in: 200 and late recomputed", r.status_code == 200 and j["late_minutes"] == 0, j)
    h = j["history"][-1] if j.get("history") else {}
    check("PATCH history: only changed fields", set(h.get("changes", {})) == {"punch_in", "late_minutes"}, h)
    check("PATCH history: epoch ms values", h["changes"]["punch_in"] == {"from": before["punch_in"], "to": new_in}, h)
    check("PATCH history: at/by/reason", isinstance(h["at"], int) and h["by"] == "hr.admin" and h["reason"] == "biometric glitch", h)
    stored = main.logs.find_one({"emp_code": "EMP5001", "date": "2026-09-09"})
    check("PATCH history stored as BSON dates", isinstance(stored["history"][0]["changes"]["punch_in"]["to"], datetime))

    r = c.patch(url, json={"punch_out": ist_ms(2026, 9, 9, 19, 0), "reason": "forgot out", "regularized_by": "hr"})
    j = r.json()
    check("PATCH punch_out: derived fields", r.status_code == 200 and j["work_hours"] == 9.53
          and j["overtime_minutes"] == 30 and len(j["history"]) == 2, j)
    bad = [({"punch_in": ist_ms(2026, 9, 10, 9, 0)}, "punch_in on another date"),
           ({"punch_out": ist_ms(2026, 9, 9, 9, 0)}, "punch_out before punch_in"),
           ({"punch_out": ist_ms(2026, 9, 10, 10, 0)}, "punch_out > 24h"),
           ({"status": "ABSENT", "punch_in": new_in}, "ABSENT with punch_in"),
           ({"status": "PRESENT"}, "changes nothing"),
           ({"punch_in": 1783312500}, "seconds timestamp"),
           ({"punch_in": None}, "explicit null")]
    for extra, name in bad:
        r = c.patch(url, json={**extra, "reason": "some reason", "regularized_by": "hr"})
        check(f"PATCH 422: {name}", r.status_code == 422, (r.status_code, r.text))
    check("PATCH 422: short reason", c.patch(url, json={"status": "WFH", "reason": "x", "regularized_by": "hr"}).status_code == 422)
    check("PATCH 404 no record", c.patch("/attendance/EMP5001/2026-01-01", json={"status": "WFH", "reason": "r e a s", "regularized_by": "hr"}).status_code == 404)
    check("PATCH 404 unknown emp", c.patch("/attendance/EMP0000/2026-09-09", json={"status": "WFH", "reason": "reason", "regularized_by": "hr"}).status_code == 404)
    check("PATCH 422 bad date", c.patch("/attendance/EMP5001/2026-9-9", json={"status": "WFH", "reason": "reason", "regularized_by": "hr"}).status_code == 422)

    r = c.patch(url, json={"status": "ABSENT", "reason": "did not come", "regularized_by": "hr"})
    j = r.json()
    check("PATCH to ABSENT clears times and derived", r.status_code == 200 and j["punch_in"] is None and j["punch_out"] is None
          and j["work_hours"] is None and j["late_minutes"] == 0 and j["overtime_minutes"] == 0, j)
    check("PATCH to ABSENT history", set(j["history"][-1]["changes"]) == {"status", "punch_in", "punch_out", "work_hours", "overtime_minutes"}, j["history"][-1])
    check("PATCH presence without punch_in 422", c.patch(url, json={"status": "PRESENT", "reason": "back again", "regularized_by": "hr"}).status_code == 422)

    # concurrent edits: every 200 must leave its own history entry
    url2 = "/attendance/EMP5001/2026-09-08"
    statuses = ["WFH", "ON_DUTY", "PRESENT", "WFH", "ON_DUTY", "PRESENT", "WFH", "ON_DUTY"]
    with ThreadPoolExecutor(8) as ex:
        res = list(ex.map(lambda s: c.patch(url2, json={"status": s, "reason": f"switch to {s}", "regularized_by": "hr"}), statuses))
    ok = sum(r.status_code == 200 for r in res)
    final = main.logs.find_one({"emp_code": "EMP5001", "date": "2026-09-08"})
    check("concurrent PATCH: no lost history", len(final.get("history", [])) == ok and ok >= 1
          and all(r.status_code in (200, 409, 422) for r in res), ([r.status_code for r in res], len(final.get("history", []))))


def test_analytics(c: httpx.Client):
    j = c.get("/analytics/employees/EMP1001/monthly", params={"month": "2026-08"}).json()
    check("monthly EMP1001", j == {"emp_code": "EMP1001", "month": "2026-08", "working_days": 21, "present_days": 3.5,
          "leave_days": 1, "late_count": 2, "total_late_minutes": 35, "total_overtime_minutes": 60, "attendance_pct": 16.67}, j)
    j = c.get("/analytics/employees/EMP1003/monthly", params={"month": "2026-08"}).json()
    check("monthly mid-month joiner", j["working_days"] == 11 and j["present_days"] == 2.0 and j["attendance_pct"] == 18.18, j)
    j = c.get("/analytics/employees/EMP1005/monthly", params={"month": "2026-08"}).json()
    check("monthly no logs", j["present_days"] == 0 and j["attendance_pct"] == 0.0 and j["late_count"] == 0, j)
    j = c.get("/analytics/employees/EMP1004/monthly", params={"month": "2026-08"}).json()
    check("monthly not yet joined -> pct null", j["working_days"] == 0 and j["attendance_pct"] is None, j)
    check("monthly 404", c.get("/analytics/employees/EMP0000/monthly", params={"month": "2026-08"}).status_code == 404)
    check("monthly 422 bad month", c.get("/analytics/employees/EMP1001/monthly", params={"month": "2026-13"}).status_code == 422)

    j = c.get("/analytics/departments/summary", params={"month": "2026-08"}).json()
    by = {i["department"]: i for i in j["items"]}
    check("summary sorted by department, future-only department omitted", [i["department"] for i in j["items"]] == ["Ops", "Punch", "QA", "Temp"], list(by))
    check("summary QA", by["QA"] == {"department": "QA", "headcount": 4, "present_days": 7.5, "avg_work_hours": 7.09,
          "late_count": 4, "total_late_minutes": 185, "leave_count": 1, "on_duty_count": 1}, by.get("QA"))
    check("summary Ops avg half-up 8.005 -> 8.01, headcount incl. no-log joiner",
          by["Ops"]["avg_work_hours"] == 8.01 and by["Ops"]["headcount"] == 4 and by["Ops"]["total_late_minutes"] == 270, by.get("Ops"))
    j = c.get("/analytics/departments/summary", params={"month": "2026-08", "department": "Future"}).json()
    check("summary: zero-headcount department omitted", j["items"] == [], j)

    lb = lambda **kw: c.get("/analytics/leaderboard/late", params={"month": "2026-08", **kw}).json()["items"]
    rows = lb(limit=2)
    check("leaderboard ties at cutoff", [(r["rank"], r["emp_code"]) for r in rows] == [(1, "EMP2001"), (2, "EMP1002"), (2, "EMP2002")], rows)
    rows = lb(limit=4)
    check("leaderboard competition ranking 1,2,2,4,4", [r["rank"] for r in rows] == [1, 2, 2, 4, 4]
          and rows[3]["emp_code"] == "EMP1003", rows)
    check("leaderboard orphan ignored", all(r["emp_code"] != "GHOST01" for r in lb(limit=50)))
    rows = lb(limit=1, department="QA")
    check("leaderboard ranks inside department", rows == [{"rank": 1, "emp_code": "EMP1002", "name": "Name EMP1002",
          "department": "QA", "total_late_minutes": 100, "late_count": 1}], rows)
    check("leaderboard late_count", next(r for r in lb(limit=50) if r["emp_code"] == "EMP2002")["late_count"] == 2)
    check("leaderboard limit 51 -> 422", c.get("/analytics/leaderboard/late", params={"month": "2026-08", "limit": 51}).status_code == 422)

    j = c.get("/analytics/departments/QA/trend", params={"from": "2026-08-14", "to": "2026-08-19"}).json()
    got = [(i["date"], i["is_working_day"], i["headcount"], i["present_count"], i["late_count"], i["attendance_rate"], i["moving_avg_7d"]) for i in j["items"]]
    exp = [("2026-08-14", True, 3, 0.0, 0, 0.0, 0.0), ("2026-08-15", False, 3, 0.0, 0, None, 0.0),
           ("2026-08-16", False, 3, 0.0, 0, None, 0.0), ("2026-08-17", True, 4, 1.0, 0, 0.25, 0.125),
           ("2026-08-18", True, 4, 1.0, 1, 0.25, 0.1667), ("2026-08-19", True, 4, 0.0, 0, 0.0, 0.125)]
    check("trend QA rows", got == exp, got)
    j = c.get("/analytics/departments/QA/trend", params={"from": "2026-08-01", "to": "2026-10-31"}).json()
    check("trend 92 days -> 92 rows", len(j["items"]) == 92, len(j.get("items", [])))
    j = c.get("/analytics/departments/QA/trend", params={"from": "2026-08-01", "to": "2026-08-07"}).json()
    sat = j["items"][0]
    check("trend weekend record counted in present_count", sat["date"] == "2026-08-01" and sat["present_count"] == 0.5
          and sat["attendance_rate"] is None and sat["moving_avg_7d"] is None, sat)
    j = c.get("/analytics/departments/Future/trend", params={"from": "2026-08-01", "to": "2026-08-03"}).json()
    check("trend nobody joined yet", [(i["headcount"], i["attendance_rate"]) for i in j["items"]] == [(0, None)] * 3, j)
    t = lambda **kw: c.get("/analytics/departments/QA/trend", params=kw).status_code
    check("trend 404 unknown department", c.get("/analytics/departments/Nope/trend", params={"from": "2026-08-01", "to": "2026-08-02"}).status_code == 404)
    check("trend 422 to < from", t(**{"from": "2026-08-05", "to": "2026-08-01"}) == 422)
    check("trend 422 93 days", t(**{"from": "2026-08-01", "to": "2026-11-01"}) == 422)
    check("trend 422 missing to", t(**{"from": "2026-08-01"}) == 422)


def test_explain(c: httpx.Client):
    cases = {
        "attendance_list": {"date_from": "2026-08-01", "date_to": "2026-08-31"},
        "employee_monthly": {"emp_code": "EMP1001", "month": "2026-08"},
        "department_summary": {"month": "2026-08"},
        "late_leaderboard": {"month": "2026-08", "limit": 3},
        "department_trend": {"department": "QA", "from": "2026-08-01", "to": "2026-08-31"},
    }
    for ep, params in cases.items():
        r = c.get(f"/admin/explain/{ep}", params=params)
        text = r.text
        check(f"explain {ep}: IXSCAN, no COLLSCAN", r.status_code == 200 and "IXSCAN" in text and "COLLSCAN" not in text,
              (r.status_code, text[:300]))
    check("explain attendance_list (no filters) IXSCAN", "IXSCAN" in c.get("/admin/explain/attendance_list").text)
    check("explain missing params 422", c.get("/admin/explain/employee_monthly", params={"month": "2026-08"}).status_code == 422)
    check("explain unknown endpoint 422", c.get("/admin/explain/whatever").status_code == 422)


def main_():
    seed()
    server = uvicorn.Server(uvicorn.Config(main.app, port=PORT, log_level="warning"))
    threading.Thread(target=server.run, daemon=True).start()
    while not server.started:
        _time.sleep(0.05)
    with httpx.Client(base_url=BASE, timeout=30) as c:
        check("health", c.get("/health").json() == {"status": "ok"})
        test_employees(c)
        test_punch(c)
        test_list_attendance(c)
        test_regularize(c)
        test_analytics(c)
        test_explain(c)
    server.should_exit = True
    print(f"\n{len(failures)} failure(s)" + (": " + ", ".join(failures) if failures else ""))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main_()
