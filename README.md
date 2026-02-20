# Gaya

**Simple data quality checks that just work.**

Gaya helps you catch data issues early with sensible defaults and zero ceremony.

```bash
pip install gaya
gaya init
gaya run
```

---

## What Gaya Checks

Out of the box, Gaya runs common, practical data quality checks with clear thresholds.

| Check | Default Behavior |
|---|---|
| Null rate per column | Warn > 10%, fail > 25% |
| Required columns | Zero nulls allowed |
| Primary key uniqueness | Zero duplicates |
| Row count change | Warn > 20%, fail > 40% |
| Schema drift | Warn on column add, fail on removal |

All thresholds are configurable in `gaya.yml`.

---

## Quick Configuration

Define your data sources and tables in a simple YAML file.

```yaml
datasources:
  main_db:
    type: postgres
    host: localhost
    database: app_db
    user: app_user
    password: env:DB_PASSWORD

tables:
  orders:
    source: main_db
    layer: staging
    primary_key: order_id
    not_null:
      - order_id
      - customer_id
```

---

## Example Output

Clear, readable output that explains what failed and why it matters.

```
  ──────────────────────────────────────────────────────
  ✖  staging.orders  FAILED
     ✖  row count dropped 38% (1.2M → 740K)
          → A drop this large usually means a failed upstream load.

  ──────────────────────────────────────────────────────
  1 table(s) · 1 failed · 7 passed
  Finished in 2.3s
  ──────────────────────────────────────────────────────
```

---

## Exit Codes

Designed to integrate cleanly with CI/CD pipelines.

| Code | Meaning |
|---|---|
| 0 | All checks passed |
| 1 | Warnings only |
| 2 | One or more checks failed |
| 3 | Gaya error (config or connection) |

---

## CI Integration

```yaml
# GitHub Actions
- name: Run data quality checks
  run: gaya run --quiet
```

---

## Supported Sources

- Postgres

Additional connectors are planned.

---

## Project Status

Gaya is an early-stage project. The core check logic, Postgres adapter, and CLI are
working. The API and configuration format may evolve, but the goal will always be the
same: simple, predictable, easy to reason about.

Feedback and contributions are welcome.
