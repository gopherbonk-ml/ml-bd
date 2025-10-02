import os, json, threading, time
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
from sqlalchemy import create_engine, text

DB_URI = os.getenv("DB_URI")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/shared/out")
os.makedirs(OUTPUT_DIR, exist_ok=True)

engine = create_engine(DB_URI, pool_pre_ping=True)
app = FastAPI(title="Lab Web API")

# буфер для «агрегации за минуту»
_buffer = []
_lock = threading.Lock()

class UserIn(BaseModel):
    name: str
    email: EmailStr
    country: str

@app.get("/users/{user_id}")
def get_user(user_id: str):
    with engine.begin() as conn:
        row = conn.execute(text("SELECT id,name,email,country,created_at FROM users WHERE id=:id"),
                           {"id": user_id}).mappings().first()
        if not row:
            raise HTTPException(404, "user not found")
        return dict(row)

@app.post("/users")
def create_user(u: UserIn):
    with engine.begin() as conn:
        row = conn.execute(
            text("""INSERT INTO users (name, email, country)
                    VALUES (:name,:email,:country)
                    RETURNING id,name,email,country,created_at"""),
            {"name": u.name, "email": u.email, "country": u.country}
        ).mappings().first()
    with _lock:
        _buffer.append({
            "id": str(row["id"]),
            "name": row["name"],
            "email": row["email"],
            "country": row["country"],
            "created_at": row["created_at"].isoformat()
        })
    return dict(row)


def _flusher():
    while True:
        time.sleep(60)
        with _lock:
            if not _buffer:
                continue
            batch = list(_buffer)
            _buffer.clear()
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(OUTPUT_DIR, f"users_{ts}.jsonl")
        with open(path, "w", encoding="utf-8") as f:
            for rec in batch:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        print(f"[flusher] wrote {len(batch)} records -> {path}")


threading.Thread(target=_flusher, daemon=True).start()
