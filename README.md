# PostgressServer

PostgreSQL-based microservice workspace for Paper Trading.

## Services

- `gateway` (port `8000`): reverse proxy entrypoint
- `services/market-backend` (port `8002`): market/ticker/fundamentals APIs
- `services/user-backend` (port `8001`): auth, wallet, portfolio, order, dashboard APIs
- Shared infra: PostgreSQL (`5432`), Redis (`6379`)

## High-Level Flow

1. Client calls gateway:
   - `/api/v1/ticker/*` -> `market-backend`
   - `/api/v1/user/*` -> `user-backend`
2. Backend services read/write PostgreSQL.
3. Redis is used for market snapshot/cache usage in services.
4. Each service has SQL migrations in `src/db/migrations`.

## Repository Structure

- `gateway/`
  - `index.js`: proxy rules
  - `.env(.example)`: gateway config
- `services/market-backend/`
  - API, workers, schedulers, migration runner, models
- `services/user-backend/`
  - API, auth/order/portfolio/wallet logic, cron jobs, migration runner, models
- `docker-compose.yml`
  - starts `postgres`, `redis`, both backends, and gateway

## Prerequisites

- Node.js 18+
- npm
- PostgreSQL 16+ (or Docker service)
- Redis 7+ (or Docker service)

## Environment Setup

Copy these files before run:

- `PostgressServer/gateway/.env.example` -> `PostgressServer/gateway/.env`
- `PostgressServer/services/market-backend/.env.example` -> `PostgressServer/services/market-backend/.env`
- `PostgressServer/services/user-backend/.env.example` -> `PostgressServer/services/user-backend/.env`

## Install

```bash
npm --prefix PostgressServer/gateway install
npm --prefix PostgressServer/services/market-backend install
npm --prefix PostgressServer/services/user-backend install
```

## Run Locally (Separate Terminals)

```bash
npm --prefix PostgressServer/services/market-backend run dev
npm --prefix PostgressServer/services/user-backend run dev
npm --prefix PostgressServer/gateway run dev
```

Gateway will expose:

- `http://localhost:8000/api/v1/ticker/*`
- `http://localhost:8000/api/v1/user/*`

## Migrations

Manual migration commands:

```bash
npm --prefix PostgressServer/services/market-backend run migrate
npm --prefix PostgressServer/services/user-backend run migrate
```

Or enable auto migration per service with:

- `PG_AUTO_MIGRATE=true`

## Schema Note

- Relational SQL migrations create and maintain tables in `public`.
- Services now persist to relational tables (no `doc`-style collection storage).

## Root Scripts

From `PostgressServer/`:

- `npm run dev:market`
- `npm run dev:user`
- `npm run dev:gateway`
- `npm run migrate:market`
- `npm run migrate:user`

## Docker

```bash
cd PostgressServer
docker compose up --build
```

Starts:

- `pt_postgres`
- `pt_redis`
- `pt_market_backend`
- `pt_user_backend`
- `pt_gateway`

## Service Documentation

- `PostgressServer/services/market-backend/README.md`
- `PostgressServer/services/user-backend/README.md`
- `PostgressServer/gateway/README.md`
