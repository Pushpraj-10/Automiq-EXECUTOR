
# Automiq — Executor Service

The executor is the execution plane. It receives execution requests from the backend over gRPC, persists them to MongoDB, and processes steps in batches. It also reports execution status back to the backend via gRPC callbacks.

## Key responsibilities

- Accept `EnqueueExecution` gRPC requests from the backend.
- Persist executions + steps to MongoDB.
- Run worker batches to execute steps (http, webhook, email, delay).
- Report execution and step status updates back to the backend.

## Requirements

- Node.js (recommended >= 18)
- MongoDB running and accessible via `MONGODB_URI`

## Environment variables

Create an `executor/.env` and set values from:

- [executor/.env.example](executor/.env.example)

Minimum required:

- `MONGODB_URI`
- `EXECUTOR_SHARED_SECRET` (must match backend)
- `BACKEND_GRPC_ADDRESS` (defaults to 127.0.0.1:50052)

## Install & run (development)

```bash
cd executor
pnpm install
pnpm dev
```

## gRPC defaults

- Executor bind: `EXECUTOR_GRPC_BIND=0.0.0.0:50051`
- Backend address: `BACKEND_GRPC_ADDRESS=127.0.0.1:50052`

## Health

- `GET /health` returns `{ ok: true, service: 'executor' }`

