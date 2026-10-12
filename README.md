# Employee Attendance & Analytics API

FastAPI + MongoDB implementation of the contract in `openapi.yaml`. All application code is in `app/main.py`.

## Run

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env               # set MONGO_URI / MONGO_DB (real env vars take priority)
python sample_seed.py              # optional: the nine sample documents
uvicorn app.main:app --port 8000
```

MongoDB 6.0+ is required (`$densify`, `$setWindowFields`, `$documents`). Local option: `docker run -p 27017:27017 mongo:7`.
Indexes are created at startup, idempotently. If MongoDB is unreachable at startup the app still starts, `/health` returns 503,
and index creation is retried on the next successful `/health`.

## Tests

```bash
pip install httpx
python tests/test_api.py           # DROPS and re-seeds the database in MONGO_TEST_DB (default: attendance_test)
```

103 end-to-end checks against a real MongoDB: R1–R5 edge cases (grace boundary, ms truncation, half-up rounding,
overnight shifts), 422/404/409 paths, races (parallel creates, punch-ins, punch-outs, PATCHes), and analytics on a seeded
dataset with ties, weekend records, a mid-month joiner, a not-yet-joined employee, an employee with zero logs, orphan logs and
legacy documents. Also checks that every `/admin/explain/*` plan has `IXSCAN` and no `COLLSCAN`.
On a generated 100k-record dataset, every endpoint answered in under ~60 ms locally, with index-only plans.

## Notes / interpretations

- Business-rule 422s (e.g. `punch_out <= punch_in`) use the same `{"detail": [{"loc", "msg", "type"}]}` shape as FastAPI validation errors.
- Trend: "range exceeds 92 days" is read as more than 92 **rows** (inclusive day count), so `from=2026-08-01&to=2026-10-31` (92 days) is accepted.
- Analytics rounding (R8) is done inside the pipelines in Decimal128 (`floor(x·10ⁿ + 0.5) / 10ⁿ`), because `$round` is banker's rounding.
- `PATCH` uses optimistic concurrency (the update is guarded by the state that was read, including `history` length). On a conflict it re-reads and re-applies, up to 5 times, then returns 409, so no history entry is ever lost silently.
- Punch-out picks the latest record with `punch_in <= punched_at` by walking the `(emp_code, date)` index backwards. Dates are monotonic with `punch_in` (one record per day), so this is equivalent to sorting on `punch_in`.
