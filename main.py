import os
import json
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import errors as pg_errors

app = FastAPI()

# Redis config
REDIS_HOST = os.getenv("REDIS_HOST", "redis.infra.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Postgres config
PG_HOST = os.getenv("POSTGRES_HOST", "postgres.infra.svc.cluster.local")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "usersdb")
PG_USER = os.getenv("POSTGRES_USER", "usersvc")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "usersvcpass")

# Redis client (single global)
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_db_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


# Pydantic models

class UserCreate(BaseModel):
    name: str
    email: EmailStr


class UserUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None


class UserOut(BaseModel):
    id: int
    name: str
    email: EmailStr


# Helper functions

def user_cache_key(user_id: int) -> str:
    return f"user:{user_id}"


def cache_user(user: dict):
    key = user_cache_key(user["id"])
    r.set(key, json.dumps(user))


def delete_user_cache(user_id: int):
    key = user_cache_key(user_id)
    r.delete(key)


def get_user_from_cache(user_id: int) -> Optional[dict]:
    key = user_cache_key(user_id)
    data = r.get(key)
    if not data:
        return None
    return json.loads(data)


@app.get("/health")
def health():
    redis_ok = False
    pg_ok = False
    try:
        r.ping()
        redis_ok = True
    except Exception:
        redis_ok = False

    try:
        conn = get_db_conn()
        conn.close()
        pg_ok = True
    except Exception:
        pg_ok = False

    return {
        "status": "ok" if (redis_ok and pg_ok) else "degraded",
        "redis": "up" if redis_ok else "down",
        "postgres": "up" if pg_ok else "down",
    }


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


# USERS CRUD (DB + Redis cache)

@app.post("/users", response_model=UserOut, status_code=201)
def create_user(payload: UserCreate):
    conn = None
    try:
        conn = get_db_conn()
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO users (name, email)
                    VALUES (%s, %s)
                    RETURNING id, name, email;
                    """,
                    (payload.name, payload.email),
                )
                user = cur.fetchone()
        # Cache the new user
        cache_user(user)
        return user
    except pg_errors.UniqueViolation:
        raise HTTPException(status_code=409, detail="Email already exists")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals():
            conn.close()


@app.get("/users", response_model=list[UserOut])
def list_users():
    conn = None
    try:
        conn = get_db_conn()
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, name, email FROM users ORDER BY id ASC;"
                )
                users = cur.fetchall()
        return users
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals():
            conn.close()


@app.get("/users/{user_id}", response_model=UserOut)
def get_user(user_id: int):
    # Try Redis cache first
    cached = get_user_from_cache(user_id)
    if cached:
        cached["_source"] = "cache"
        return cached

    # Fallback to DB
    conn = None
    try:
        conn = get_db_conn()
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, name, email FROM users WHERE id = %s;",
                    (user_id,),
                )
                user = cur.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Cache and return
        cache_user(user)
        user["_source"] = "db"
        return user
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals():
            conn.close()


@app.put("/users/{user_id}", response_model=UserOut)
def update_user(user_id: int, payload: UserUpdate):
    conn = None
    try:
        conn = get_db_conn()
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Fetch existing
                cur.execute(
                    "SELECT id, name, email FROM users WHERE id = %s;",
                    (user_id,),
                )
                existing = cur.fetchone()
                if not existing:
                    raise HTTPException(status_code=404, detail="User not found")

                new_name = payload.name if payload.name is not None else existing["name"]
                new_email = payload.email if payload.email is not None else existing["email"]

                cur.execute(
                    """
                    UPDATE users
                    SET name = %s, email = %s, updated_at = NOW()
                    WHERE id = %s
                    RETURNING id, name, email;
                    """,
                    (new_name, new_email, user_id),
                )
                user = cur.fetchone()

        # Update cache
        cache_user(user)
        return user
    except pg_errors.UniqueViolation:
        raise HTTPException(status_code=409, detail="Email already exists")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals():
            conn.close()


@app.delete("/users/{user_id}", status_code=204)
def delete_user(user_id: int):
    conn = None
    try:
        conn = get_db_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM users WHERE id = %s;", (user_id,))
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="User not found")

        # Remove from cache
        delete_user_cache(user_id)
        return
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals():
            conn.close()

