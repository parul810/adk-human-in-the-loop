# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import sqlite3
import uuid
from typing import Literal, Optional

import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel

logger = logging.getLogger("uvicorn.error")

app = FastAPI(title="Expense Request API")

# ADK server (`adk web` / `adk api_server`) that receives the decision webhook
ADK_URL = os.getenv("ADK_URL", "http://localhost:8000")
DB_PATH = os.getenv("APPROVAL_DB_PATH", "approvals.db")

class Callback(BaseModel):
    """Where to deliver the decision: the paused ADK function call."""
    app_name: str
    user_id: str
    session_id: str
    function_call_id: str
    function_name: str

class RequestData(BaseModel):
    amount: float
    reason: str
    callback: Optional[Callback] = None

class ResponseData(BaseModel):
    id: str
    status: str
    amount: float
    reason: str
    message: str

class StatusUpdate(BaseModel):
    status: Literal["approved", "rejected"]

# Requests are stored in SQLite so they survive a server restart
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

with db() as conn:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS requests (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            amount REAL NOT NULL,
            reason TEXT NOT NULL,
            message TEXT NOT NULL,
            callback TEXT
        )"""
    )

def to_response(row: sqlite3.Row) -> ResponseData:
    return ResponseData(
        id=row["id"],
        status=row["status"],
        amount=row["amount"],
        reason=row["reason"],
        message=row["message"],
    )

def get_row(request_id: str) -> sqlite3.Row:
    with db() as conn:
        row = conn.execute("SELECT * FROM requests WHERE id = ?", (request_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Request not found")
    return row

def set_message(request_id: str, message: str) -> None:
    with db() as conn:
        conn.execute("UPDATE requests SET message = ? WHERE id = ?", (message, request_id))

async def resume_agent(request_id: str, callback: Callback, status: str, amount: float, reason: str) -> None:
    """Webhook: send the decision to ADK as the response to the paused function call."""
    payload = {
        "app_name": callback.app_name,
        "user_id": callback.user_id,
        "session_id": callback.session_id,
        "new_message": {
            "role": "user",
            "parts": [{
                "function_response": {
                    "id": callback.function_call_id,
                    "name": callback.function_name,
                    "response": {
                        "status": status,
                        "approval_request_id": request_id,
                        "amount": amount,
                        "reason": reason,
                    },
                }
            }],
        },
    }
    try:
        # The agent runs to completion inside this call, so allow LLM time
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.post(f"{ADK_URL}/run", json=payload)
            response.raise_for_status()
        set_message(request_id, f"Request {status}; agent resumed")
    except Exception as e:
        logger.exception("Failed to resume agent for request %s", request_id)
        set_message(request_id, f"Request {status}; failed to resume agent: {e}")

@app.post("/request", response_model=ResponseData)
async def handle_request(data: RequestData):
    request_id = str(uuid.uuid4())
    with db() as conn:
        conn.execute(
            "INSERT INTO requests (id, status, amount, reason, message, callback) VALUES (?, ?, ?, ?, ?, ?)",
            (
                request_id,
                "pending",
                data.amount,
                data.reason,
                "Request created successfully",
                data.callback.model_dump_json() if data.callback else None,
            ),
        )
    return to_response(get_row(request_id))

@app.get("/requests", response_model=list[ResponseData])
async def get_requests():
    with db() as conn:
        rows = conn.execute("SELECT * FROM requests ORDER BY rowid").fetchall()
    return [to_response(row) for row in rows]

@app.get("/request/{request_id}", response_model=ResponseData)
async def get_request_by_id(request_id: str):
    return to_response(get_row(request_id))

@app.put("/request/{request_id}", response_model=ResponseData)
async def update_request(request_id: str, status_update: StatusUpdate, background_tasks: BackgroundTasks):
    row = get_row(request_id)
    # Only a pending request can be decided, and only once
    with db() as conn:
        updated = conn.execute(
            "UPDATE requests SET status = ? WHERE id = ? AND status = 'pending'",
            (status_update.status, request_id),
        ).rowcount
    if not updated:
        raise HTTPException(status_code=409, detail=f"Request already {get_row(request_id)['status']}")

    # Resume the agent after responding, so the approver isn't kept waiting
    if row["callback"]:
        background_tasks.add_task(
            resume_agent,
            request_id,
            Callback.model_validate_json(row["callback"]),
            status_update.status,
            row["amount"],
            row["reason"],
        )
    return to_response(get_row(request_id))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)
