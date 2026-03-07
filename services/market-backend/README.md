# market-backend

Market microservice for Paper Trading with PostgreSQL persistence.

## Responsibilities

- Stock master/admin APIs
- Active stock APIs
- Fundamentals APIs and worker/scheduler integration
- EOD OHLC APIs
- Token/market related service operations

## Routes

Configured in `src/routes/index.route.js`:

- `/admin`
- `/master`
- `/activestock`
- `/fundamentals`
- `/eod`

When accessed through gateway, route prefix becomes:

- `/api/v1/ticker/*`

## Project Structure

- `src/server.js`
  - service bootstrap
- `src/app.js`
  - express app + route registration
- `src/config/`
  - env, db/redis/socket config
- `src/controllers/`
  - request handlers
- `src/routes/`
  - express routes
- `src/services/`
  - business logic
- `src/repositories/`
  - PostgreSQL query layer (table-specific data access)
- `src/db/`
  - pg client + migration runner
- `src/db/migrations/`
  - SQL schema files
- `src/workers/`
  - background workers (fundamentals)
- `src/schedulers/`
  - cron/scheduling logic
- `src/pythonApi/`
  - integration with python engine API
- `src/socket/`
  - socket-related logic
- `src/validator/`
  - request validation rules
- `src/utils/`
  - shared helpers/constants

## Environment Variables

From `.env.example`:

- `PORT`
- `PG_DATABASE_URL`
- `PG_SSL`
- `PG_AUTO_MIGRATE`
- `PG_AUTO_CREATE_DB`
- `REDIS_HOST`
- `REDIS_PORT`
- `SUPERADMIN_KEY`
- `FUNDAMENTALS_SCHEDULER_ROLE`
- `FUNDAMENTALS_REFRESH_DAYS`
- `FUNDAMENTALS_QUEUE_BATCH`
- `PYTHON_API_BASE_URL`
- `SMARTAPI_API_KEY`
- `SMARTAPI_CLIENT_CODE`
- `SMARTAPI_PASSWORD`

## Scripts

- `npm run dev`
  - start with nodemon
- `npm run start`
  - start with node
- `npm run migrate`
  - run SQL migrations
- `npm run worker:fundamentals`
  - start fundamentals worker directly
- `npm run seed:rawstocks`
  - fetch and upsert rawstocks master list

## Migration Notes

- SQL files are in `src/db/migrations/`.
- `npm run migrate` applies pending files in order.
- If `PG_AUTO_MIGRATE=true`, migration runner executes at boot.
- If `PG_AUTO_CREATE_DB=true`, service auto-creates target database when missing.

## Local Run

```bash
npm install
npm run migrate
npm run dev
```

## Health

Use service health route (or via gateway) after startup.
