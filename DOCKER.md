# Docker setups

The repository ships three separate Docker entry points because the workflows
serve different purposes.

## 1. Main evaluation stack: `docker-compose-multidb.yml`

Use this for the primary schedule-exploration workflow.

```bash
docker compose -f docker-compose-multidb.yml up -d
docker compose -f docker-compose-multidb.yml down -v
```

This stack provides:

- PostgreSQL 12.3 and 12.4 for the `update_conflict_out` case study
- PostgreSQL 16.2 as a modern baseline
- MySQL 8.0.34 for the current MySQL case study
- MySQL 5.6 and 5.7 for historical bug-comparison workflows

Use it with:

```bash
python scripts/experiments/run_final_eval.py --quick --db postgres mysql sqlite
```

## 2. Focused PostgreSQL repro stack: `docker-compose-repro.yml`

Use this when you only want the PostgreSQL 12.3/12.4 reproduction pair.

```bash
docker compose -f docker-compose-repro.yml up -d
docker compose -f docker-compose-repro.yml down -v
```

This is the smallest container setup for the historical
`update_conflict_out` reproduction workflow.

## 3. Helper stack for litmus utilities: `docker-compose.yml`

Use this for auxiliary MySQL and Redis helper scripts.

```bash
docker compose up -d
docker compose down
```

Services:

| Service | Container | Port | Typical use |
|---------|-----------|------|-------------|
| MySQL | `mysql-litmus` | `3307` | litmus and mixed-attack scripts |
| Redis | `redis-litmus` | `6380` | Redis helper workflows |

See [scripts/mysql.md](scripts/mysql.md) and [scripts/redis.md](scripts/redis.md)
for those auxiliary workflows.
