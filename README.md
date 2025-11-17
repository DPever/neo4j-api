# Neo4j API (Express)

A minimal Node.js API exposing read/write endpoints for a Neo4j backend.

## Quick start

1) **Create project**

```bash
cd neo4j-api
npm install
cp .env.example .env
# edit .env and set NEO4J_URI/NEO4J_USERNAME/NEO4J_PASSWORD
```

2) **Run**

```bash
npm run start
# or auto-reload
npm run dev
```

3) **Endpoints**

- `GET /health`
- `GET /locations/:name`
- `GET /path?from=SPARTA-MUSKEGON&to=PATTERSON/TRUNKLINE DEL&at=2025-11-01T00:00:00Z&maxHops=6`
- `GET /constraints?location=SPARTA-MUSKEGON&at=2025-11-12T22:13:16Z&limit=25&skip=0`
- `POST /constraints` (requires `ENABLE_WRITES=true`)
  ```json
  {
    "reason": "Maintenance",
    "kind": "Capacity",
    "start": "2025-11-01T00:00:00Z",
    "end": null,
    "limit": 1000,
    "locationName": "SPARTA-MUSKEGON"
  }
  ```
- `PATCH /constraints/set-createdAt-from-start` (requires `ENABLE_WRITES=true`)
- `POST /cypher/read` (read-only, parameterized)

## Notes

- The API purposefully defaults to **read-only** (`ENABLE_WRITES=false`). Flip the flag when you need to create/update data.
- Timestamps are handled as `datetime()` inside Cypher; pass ISO-8601 strings.
- The `/path` endpoint mirrors the Bloom-style query you were using and can optionally mark `constrained` nodes for a given `at` time.
- Safe for Neo4j Aura or self-hosted. Use `neo4j+s://` for Aura with encryption.

## Deploy

- **Local**: `npm run start`
- **Docker (example)**: Create a Dockerfile:  
  ```Dockerfile
  FROM node:20-alpine
  WORKDIR /app
  COPY package.json package-lock.json* ./
  RUN npm install --omit=dev
  COPY . .
  EXPOSE 8080
  CMD ["npm","start"]
  ```
  Then build and run:
  ```bash
  docker build -t neo4j-api .
  docker run --env-file .env -p 8080:8080 neo4j-api
  ```

## Postman

A starter Postman collection is included at the repo root (`neo4j-neo4j.postman_collection.json`). Import it and edit the variables (baseUrl, etc.).
