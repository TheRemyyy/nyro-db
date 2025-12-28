# Real-Time API

NyroDB includes a native WebSocket server for broadcasting real-time updates to connected clients. This eliminates the need for external message queues like Redis Pub/Sub for simple use cases.

## Connection

- **URL**: `ws://127.0.0.1:8081/ws`
- **Handshake**: Standard HTTP Upgrade header.

## Protocol format

NyroDB uses a simple text-based protocol for server-to-client messages.

### Message Structure

```text
EVENT_TYPE:MODEL_NAME:JSON_PAYLOAD
```

### Supported Events

#### `INSERT`

Broadcasted whenever a new record is successfully written to the database.

- **Format**: `INSERT:<model_name>:<json_object>`
- **Example**:

  ```text
  INSERT:user:{"id":105,"email":"alice@example.com","created_at":1678899000}
  ```

## Client Implementation Example (JavaScript)

```javascript
const ws = new WebSocket('ws://localhost:8081/ws');

ws.onopen = () => {
    console.log('Connected to NyroDB Real-Time Stream');
};

ws.onmessage = (event) => {
    const raw = event.data;
    const parts = raw.split(':', 2); // Split only on first two colons
    
    if (parts.length < 2) return;
    
    const eventType = parts[0];
    const modelName = parts[1];
    // The rest of the string is JSON
    const jsonStr = raw.substring(parts[0].length + parts[1].length + 2);
    
    if (eventType === 'INSERT') {
        const data = JSON.parse(jsonStr);
        console.log(`New entry in ${modelName}:`, data);
        // Update UI...
    }
};
```
