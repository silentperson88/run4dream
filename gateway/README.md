# gateway

API gateway (reverse proxy) for Paper Trading microservices.

## Responsibilities

- Single entrypoint for client apps
- Path-based routing to internal services
- Keeps backend service URLs hidden from clients

## Proxy Mapping

Defined in `index.js`:

- `/api/v1/ticker/*` -> `MARKET_BACKEND_URL` (default `http://localhost:8002`)
- `/api/v1/user/*` -> `USER_BACKEND_URL` (default `http://localhost:8001`)

Path rewriting:

- `/api/v1/ticker` prefix removed before forwarding
- `/api/v1/user` prefix removed before forwarding

## Files

- `index.js`
  - express app and proxy middleware setup
- `.env(.example)`
  - runtime configuration
- `Dockerfile`
  - container runtime image
- `package.json`
  - scripts/dependencies

## Environment Variables

- `PORT`
- `MARKET_BACKEND_URL`
- `USER_BACKEND_URL`

## Scripts

- `npm run dev`
- `npm run start`

## Local Run

```bash
npm install
npm run dev
```
