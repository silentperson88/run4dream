# user-backend

User domain microservice for Paper Trading with PostgreSQL persistence.

## Responsibilities

- Authentication and email verification
- Password reset
- Wallet load/transfer/withdraw flows
- Portfolio create/list/holdings/summary/archive
- Order placement and order lifecycle endpoints
- Dashboard aggregates
- Order execution cron for open/partially-filled orders

## Routes

Configured in `src/routes/index.route.js`:

- `/auth`
- `/portfolio-types`
- `/my-portfolios`
- `/wallet`
- `/dashboard`
- `/order`

When accessed through gateway, route prefix becomes:

- `/api/v1/user/*`

## Project Structure

- `src/server.js`
  - service bootstrap + DB init import
- `src/app.js`
  - express app + route registration + cron startup
- `src/config/`
  - environment/db/redis configuration
- `src/controllers/`
  - HTTP controller layer
- `src/routes/`
  - route definitions
- `src/services/`
  - business/domain logic
- `src/repositories/`
  - PostgreSQL query layer (normalized DB access)
- `src/db/`
  - pg pool + migration runner
- `src/db/migrations/`
  - SQL migration files for schema
- `src/cron/`
  - order execution scheduler
- `src/middlewares/`
  - auth/role/validation middlewares
- `src/validator/`
  - express-validator schemas
- `src/utils/`
  - jwt, response helpers, constants, email templates

## Environment Variables

From `.env.example`:

- `PORT`
- `PG_DATABASE_URL`
- `PG_SSL`
- `PG_AUTO_MIGRATE`
- `PG_AUTO_CREATE_DB`
- `JWT_SECRET`
- `JWT_EXPIRES_IN`
- `REDIS_HOST`
- `REDIS_PORT`
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USER`
- `SMTP_PASS`
- `SMTP_FROM`
- `APP_NAME`
- `APP_URL`

## Scripts

- `npm run dev`
  - start with nodemon
- `npm run start`
  - start with node
- `npm run migrate`
  - run SQL migrations
- `npm run seeder`
  - seed portfolio types
- `npm run seed:portfolio-types`
  - seed portfolio types (alias)

## Migration Notes

- SQL files are in `src/db/migrations/`.
- `npm run migrate` applies pending SQL files.
- If `PG_AUTO_MIGRATE=true`, migrations run on startup.
- If `PG_AUTO_CREATE_DB=true`, service auto-creates target database when missing.

## Storage/Model Note

- Service model operations now map to relational tables in `public`.

## Local Run

```bash
npm install
npm run migrate
npm run dev
```

## Health

Service health endpoint:

- `GET /health`
