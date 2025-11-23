import os
from fastapi import FastAPI, HTTPException
import redis

app = FastAPI()

REDIS_HOST = os.getenv("REDIS_HOST", "redis.infra.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

@app.get("/health")
def health():
    try:
        r.ping()
        return {"status": "ok", "redis": "up"}
    except Exception as e:
        return {"status": "degraded", "redis_error": str(e)}

@app.post("/set")
def set_value(key: str, value: str):
    r.set(key, value)
    return {"key": key, "value": value}

@app.get("/get")
def get_value(key: str):
    val = r.get(key)
    if val is None:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"key": key, "value": val}
