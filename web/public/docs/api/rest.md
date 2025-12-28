# REST API Reference

NyroDB provides a straightforward JSON-based REST API for data manipulation and querying.

**Base URL**: `http://127.0.0.1:8081`

## Authentication

If `security.enable_auth` is `true` in `nyrodb.toml`, all requests must include:
`x-api-key: <your_api_key>`

## Endpoints

### 1. Insert Data

Write a new record to the database.

- **URL**: `POST /insert/:model`
- **Body**: JSON Object matching the model's schema.
- **Response**:

  ```json
  { "id": 123 }
  ```

### 2. Get by ID

Retrieve a single record using its primary key.

- **URL**: `GET /get/:model/:id`
- **Response**:

  ```json
  {
    "id": 123,
    "email": "user@example.com",
    ...
  }
  ```

- **Errors**: `404 Not Found` if ID does not exist.

### 3. Query All

Retrieve all records for a given model.

- **URL**: `GET /query/:model`
- **Response**: JSON Array of objects.

### 4. Secondary Index Query (O(1))

Instantly retrieve records matching a specific field value.

- **URL**: `GET /query/:model/:field/:value`
- **Example**: `GET /query/user/email/alice@example.com`
- **Performance**: This operation is O(1) regardless of dataset size due to hash indexing.

### 5. Config & Meta

- `GET /config`: Returns the current active configuration.
- `GET /models`: Returns a list of available model names.

### 6. Metrics

- **URL**: `GET /metrics`
- **Response**:

  ```json
  {
    "uptime_seconds": 3600,
    "total_requests": 15420,
    "ops_per_sec": 4200.5,
    "p99_latency_ms": 0.05
  }
  ```
