# DECISIONS.md

1. **Indexes.** `employees`: unique `emp_code` (identity, race-safe create, leaderboard `$lookup`); `(department, joined_on)` (trend and per-department summary headcount); `joined_on` (all-department summary). `attendance_logs`: unique `(emp_code, date)` (one record per day, punch-in race, punch-out, PATCH, monthly, the summary/trend `$lookup`s); `(date desc, emp_code)` (default list order, leaderboard month range). I rejected a `status` index: status has five values, so on its own it filters too little to justify the write cost.

2. **Punch-in race.** Both requests find the employee and compute the same `(emp_code, date)`. Both call `insert_one`. The unique index lets exactly one insert through. The other gets `DuplicateKeyError`, which I turn into 409. Nothing is checked before the insert.

3. **Ties.** `$rank` assigns competition ranks (1, 2, 2, 4), and `limit` filters on `rank <= limit`. Both tied employees are returned, even if that means more than `limit` rows, ordered by `emp_code`.

4. **Headcount.** The pipeline starts from `employees` (`joined_on <= month end`) and `$lookup`s that month's logs. An employee with no logs still produces one row with an empty array, which counts toward headcount.

5. **At 100x.** Precompute daily per-department aggregates when punches happen, instead of re-aggregating raw logs on every request.
