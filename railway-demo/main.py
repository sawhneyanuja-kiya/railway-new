"""FastAPI entry point: run with ``uvicorn main:app --reload``."""

from collections import deque
from datetime import datetime
from itertools import count
from pathlib import Path
import re
from threading import Lock
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from booking import BookingSimulator
from railway_db import DATABASE_PATH, RailwayDatabase


ROOT = Path(__file__).resolve().parent
FRONTEND = ROOT / "frontend"
database = RailwayDatabase(DATABASE_PATH)
simulator = BookingSimulator(database)

app = FastAPI(
    title="Smart Railway Directory & Concurrent Ticket Booking System",
    description="An interactive railway directory and OS process-synchronization lab.",
    version="1.0.0",
)


class BookingRequest(BaseModel):
    seat_id: int
    passenger: str = Field(min_length=2, max_length=60)


class CancelRequest(BaseModel):
    seat_id: int


class SimulationRequest(BaseModel):
    train_no: int
    seat_ids: list[int] = Field(min_length=1, max_length=5)
    users: int = Field(ge=2, le=30)
    mode: Literal["unsafe", "mutex", "semaphore"]


class LoginRequest(BaseModel):
    username: str = Field(min_length=2, max_length=60)
    password: str = Field(min_length=1, max_length=100)


_event_lock = Lock()
_events: deque[dict[str, str | int]] = deque(maxlen=500)
_event_ids = count(1)
_process_ids = count(1)


def new_process_id() -> str:
    with _event_lock:
        return f"Process-{next(_process_ids)}"


def add_event(process: str, message: str, kind: str = "info") -> None:
    with _event_lock:
        _events.append(
            {
                "id": next(_event_ids),
                "time": datetime.now().astimezone().strftime("%H:%M:%S.%f")[:-3],
                "process": process,
                "message": message,
                "kind": kind,
            }
        )


@app.get("/", include_in_schema=False)
def home() -> FileResponse:
    return FileResponse(FRONTEND / "index.html")


@app.post("/api/login")
def login(request: LoginRequest) -> dict[str, str | bool]:
    username = re.sub(r"\s+", " ", request.username).strip()
    if len(username) < 2:
        raise HTTPException(status_code=422, detail="Enter a valid display name.")
    process = new_process_id()
    add_event(process, f"{username} opened the railway dashboard.")
    return {"success": True, "username": username}


@app.get("/api/log")
def live_log(since: int = Query(default=0, ge=0)) -> dict[str, list[dict[str, str | int]]]:
    with _event_lock:
        events = [event for event in _events if event["id"] > since]
    return {"events": events}


@app.get("/api/trains")
def list_trains(search: str = Query(default="", max_length=100)) -> dict:
    return {"trains": database.list_trains(search)}


@app.get("/api/trains/{train_no}/layout")
def train_layout(train_no: int) -> dict:
    layout = database.get_layout(train_no)
    if layout is None:
        raise HTTPException(status_code=404, detail="Train not found.")
    return layout


@app.post("/api/book")
def book_seat(request: BookingRequest) -> dict:
    seat = database.get_seat(request.seat_id)
    if not seat:
        raise HTTPException(status_code=404, detail="Berth not found.")
    passenger = re.sub(r"\s+", " ", request.passenger).strip()
    if len(passenger) < 2:
        raise HTTPException(status_code=422, detail="Enter a valid passenger name.")
    process = new_process_id()
    seat_label = f"{seat['coach_code']}-{seat['seat_no']:02d}"
    booked = simulator.book_one(
        request.seat_id,
        passenger,
        seat_label,
        lambda message, kind: add_event(process, message, kind),
    )
    if not booked:
        raise HTTPException(status_code=409, detail="This berth is already booked.")
    return {
        "success": True,
        "message": f"Berth confirmed for {passenger}.",
        "layout": database.get_layout(seat["train_no"]),
    }


@app.post("/api/cancel")
def cancel_seat(request: CancelRequest) -> dict:
    seat = database.get_seat(request.seat_id)
    if not seat:
        raise HTTPException(status_code=404, detail="Berth not found.")
    process = new_process_id()
    seat_label = f"{seat['coach_code']}-{seat['seat_no']:02d}"
    cancelled = simulator.cancel_one(
        request.seat_id,
        seat_label,
        lambda message, kind: add_event(process, message, kind),
    )
    if not cancelled:
        raise HTTPException(status_code=409, detail="This berth has no booking to cancel.")
    return {
        "success": True,
        "message": f"Booking cancelled for {seat_label}.",
        "layout": database.get_layout(seat["train_no"]),
    }


@app.post("/api/simulate")
async def simulate(request: SimulationRequest) -> dict:
    try:
        result = await run_in_threadpool(
            simulator.run,
            request.train_no,
            request.seat_ids,
            request.users,
            request.mode,
        )
        for event in result["events"]:
            add_event(event["thread"], event["message"], event["kind"])
        return result
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@app.post("/api/reset")
def reset_demo_data() -> dict:
    database.reset_demo_data()
    return {"success": True, "message": "Demo bookings restored.", "trains": database.list_trains()}


@app.get("/api/code/booking.py")
def booking_source() -> dict:
    source = (ROOT / "booking.py").read_text(encoding="utf-8").splitlines()
    highlights = [
        number
        for number, line in enumerate(source)
        if any(
            marker in line
            for marker in (
                "def _run_unsafe(",
                "def _run_mutex(",
                "def _run_semaphore(",
                "with seat_lock:",
                "empty = Semaphore(capacity)",
                "full = Semaphore(0)",
            )
        )
    ]
    return {"filename": "booking.py", "lines": source, "highlights": highlights}
