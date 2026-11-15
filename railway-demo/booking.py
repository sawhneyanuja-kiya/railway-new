"""Concurrent booking simulations used by the Railway Directory dashboard."""

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event, Lock, Semaphore
from time import perf_counter, sleep
from typing import Any, Callable, Literal

from railway_db import RailwayDatabase


Mode = Literal["unsafe", "mutex", "semaphore"]


class BookingSimulator:
    def __init__(self, database: RailwayDatabase, io_delay: float = 0.08) -> None:
        self.database = database
        self.io_delay = io_delay
        self._seat_locks: dict[int, Lock] = {}
        self._seat_locks_guard = Lock()

    def _lock_for(self, seat_id: int) -> Lock:
        with self._seat_locks_guard:
            return self._seat_locks.setdefault(seat_id, Lock())

    def book_one(
        self,
        seat_id: int,
        passenger: str,
        seat_label: str,
        emit: Callable[[str, str], None],
    ) -> bool:
        """Book one berth while emitting each mutex lifecycle event."""
        seat_lock = self._lock_for(seat_id)
        emit(f"Attempting to acquire Mutex lock for {seat_label}...", "waiting")
        if seat_lock.locked():
            emit("Lock busy; waiting for the current booking to finish.", "waiting")
        with seat_lock:
            emit(f"Lock acquired. Checking {seat_label}...", "entered")
            try:
                if self.database.seat_status(seat_id) != "AVAILABLE":
                    emit(f"Lock busy, {seat_label} already booked.", "rejected")
                    return False
                emit(f"Booking {seat_label} for {passenger}...", "read")
                if not self.database.book_if_available(seat_id, passenger):
                    emit(f"Lock busy, {seat_label} already booked.", "rejected")
                    return False
                emit(f"Booking confirmed: {seat_label} → {passenger}.", "booked")
                return True
            finally:
                emit("Lock released.", "info")

    def cancel_one(
        self,
        seat_id: int,
        seat_label: str,
        emit: Callable[[str, str], None],
    ) -> int:
        """Cancel bookings for one berth while holding its mutex."""
        seat_lock = self._lock_for(seat_id)
        emit(f"Attempting to acquire Mutex lock to cancel {seat_label}...", "waiting")
        if seat_lock.locked():
            emit("Lock busy; waiting for the current booking to finish.", "waiting")
        with seat_lock:
            emit(f"Lock acquired. Checking {seat_label}...", "entered")
            try:
                cancelled = self.database.cancel_bookings(seat_id)
                if cancelled:
                    emit(f"Cancelled {cancelled} booking(s) for {seat_label}.", "booked")
                else:
                    emit(f"{seat_label} is already available; nothing to cancel.", "rejected")
                return cancelled
            finally:
                emit("Lock released.", "info")

    def _run_unsafe(self, seat_id: int, passenger: str, barrier: Barrier,
                    thread_name: str, events: list[dict[str, str]],
                    events_lock: Lock) -> bool:
        """Check then act without a lock so competing requests can overwrite."""
        self._log(events, events_lock, thread_name, "entered",
                  "Entered critical section (no lock)")
        status = self.database.seat_status(seat_id)
        self._log(events, events_lock, thread_name, "read",
                  f"Read status='{status}'")
        if status != "AVAILABLE":
            self._log(events, events_lock, thread_name, "rejected",
                      "Rejected: berth was already booked")
            return False
        sleep(self.io_delay)
        barrier.wait(timeout=10)
        self.database.insert_booking(seat_id, passenger)
        self._log(events, events_lock, thread_name, "conflict",
                  f"CONFLICT: wrote booking to seat {seat_id} without a lock")
        return True

    def _run_mutex(self, seat_id: int, passenger: str, thread_name: str,
                   events: list[dict[str, str]], events_lock: Lock) -> bool:
        """Protect the complete read-check-write section with a per-seat mutex."""
        seat_lock = self._lock_for(seat_id)
        if seat_lock.locked():
            self._log(events, events_lock, thread_name, "waiting",
                      "Blocked waiting for this seat's mutex")
        with seat_lock:
            self._log(events, events_lock, thread_name, "entered",
                      "Entered critical section (seat mutex acquired)")
            status = self.database.seat_status(seat_id)
            self._log(events, events_lock, thread_name, "read",
                      f"Read status='{status}'")
            if status != "AVAILABLE":
                self._log(events, events_lock, thread_name, "rejected",
                          "Rejected: another passenger has this berth")
                return False
            sleep(self.io_delay)
            self.database.insert_booking(seat_id, passenger)
            self._log(events, events_lock, thread_name, "booked",
                      f"Confirmed booking on seat {seat_id}; mutex released")
            return True

    def _run_semaphore(self, jobs: list[tuple[int, str, str]],
                       events: list[dict[str, str]], events_lock: Lock,
                       capacity: int = 5, workers: int = 2) -> int:
        """Consume passenger requests from a semaphore-bounded FIFO buffer."""
        buffer: deque[tuple[int, str, str] | None] = deque()
        empty = Semaphore(capacity)
        full = Semaphore(0)
        mutex = Lock()
        producer_done = Event()
        max_size = 0

        def produce() -> None:
            nonlocal max_size
            try:
                for job in jobs:
                    if not empty.acquire(timeout=0.02):
                        self._log(events, events_lock, job[2], "waiting",
                                  "WAITING (Queue FULL) — producer needs a free slot")
                        empty.acquire()
                    with mutex:
                        buffer.append(job)
                        buffer_size = len(buffer)
                        max_size = max(max_size, buffer_size)
                    full.release()
                    self._log(events, events_lock, job[2], "queued",
                              f"Request entered bounded queue ({buffer_size}/{capacity})")
            finally:
                producer_done.set()
                for _ in range(workers):
                    empty.acquire()
                    with mutex:
                        buffer.append(None)
                    full.release()

        def consume(worker_no: int) -> None:
            waiting_logged = False
            while True:
                if not full.acquire(timeout=0.02):
                    if producer_done.is_set():
                        return
                    if not waiting_logged:
                        self._log(events, events_lock, f"Counter-{worker_no}", "waiting",
                                  "WAITING (Queue EMPTY) — counter awaits a request")
                        waiting_logged = True
                    continue
                waiting_logged = False
                with mutex:
                    job = buffer.popleft()
                empty.release()
                if job is None:
                    return
                seat_id, passenger, thread_name = job
                seat_lock = self._lock_for(seat_id)
                with seat_lock:
                    self._log(events, events_lock, thread_name, "entered",
                              f"Counter-{worker_no} entered seat critical section")
                    status = self.database.seat_status(seat_id)
                    self._log(events, events_lock, thread_name, "read",
                              f"Read status='{status}'")
                    if status != "AVAILABLE":
                        self._log(events, events_lock, thread_name, "rejected",
                                  "Rejected: berth already sold")
                        continue
                    sleep(self.io_delay)
                    self.database.insert_booking(seat_id, passenger)
                    self._log(events, events_lock, thread_name, "booked",
                              f"Ticket issued for seat {seat_id} by Counter-{worker_no}")

        with ThreadPoolExecutor(max_workers=workers + 1) as pool:
            consumers = [pool.submit(consume, number) for number in range(1, workers + 1)]
            producer = pool.submit(produce)
            producer.result()
            for consumer in consumers:
                consumer.result()
        return max_size

    @staticmethod
    def _log(events: list[dict[str, str]], lock: Lock, thread: str,
             kind: str, message: str) -> None:
        with lock:
            events.append({"thread": thread, "kind": kind, "message": message})

    def run(self, train_no: int, seat_ids: list[int], users: int,
            mode: Mode) -> dict[str, Any]:
        if not 2 <= users <= 30:
            raise ValueError("Concurrent users must be between 2 and 30.")
        if not seat_ids or len(seat_ids) > 5 or len(set(seat_ids)) != len(seat_ids):
            raise ValueError("Select between one and five distinct target seats.")
        layout = self.database.get_layout(train_no)
        if layout is None:
            raise ValueError("Train not found.")
        valid_ids = {
            seat["seat_id"]
            for coach in layout["coaches"]
            for seat in coach["seats"]
        }
        if any(seat_id not in valid_ids for seat_id in seat_ids):
            raise ValueError("Every target berth must belong to the selected train.")

        grouped_jobs: dict[int, list[tuple[int, str, str]]] = {
            seat_id: [] for seat_id in seat_ids
        }
        for index in range(users):
            seat_id = seat_ids[index % len(seat_ids)]
            grouped_jobs[seat_id].append(
                (seat_id, f"Passenger {index + 1:02d}", f"Thread-{index + 1:02d}")
            )
        events: list[dict[str, str]] = []
        events_lock = Lock()
        started = perf_counter()
        confirmed = 0
        rejected = 0
        max_queue_size = 0

        if mode == "unsafe":
            barriers = {
                seat_id: Barrier(len(group))
                for seat_id, group in grouped_jobs.items()
            }
            futures = []
            with ThreadPoolExecutor(max_workers=users) as pool:
                for group in grouped_jobs.values():
                    for seat_id, passenger, thread_name in group:
                        futures.append(
                            pool.submit(
                                self._run_unsafe, seat_id, passenger,
                                barriers[seat_id], thread_name, events, events_lock,
                            )
                        )
                outcomes = [future.result() for future in futures]
            confirmed = sum(outcomes)
            rejected = users - confirmed
        elif mode == "mutex":
            futures = []
            with ThreadPoolExecutor(max_workers=users) as pool:
                for group in grouped_jobs.values():
                    for seat_id, passenger, thread_name in group:
                        futures.append(
                            pool.submit(
                                self._run_mutex, seat_id, passenger,
                                thread_name, events, events_lock,
                            )
                        )
                outcomes = [future.result() for future in futures]
            confirmed = sum(outcomes)
            rejected = users - confirmed
        else:
            jobs = [
                job for group in grouped_jobs.values() for job in group
            ]
            max_queue_size = self._run_semaphore(jobs, events, events_lock)
            confirmed = sum(event["kind"] == "booked" for event in events)
            rejected = users - confirmed

        elapsed_ms = round((perf_counter() - started) * 1000, 2)
        return {
            "mode": mode,
            "total_requests": users,
            "confirmed": confirmed,
            "rejected": rejected,
            "conflicts": self.database.count_conflicts(seat_ids),
            "execution_time_ms": elapsed_ms,
            "queue_max_size": max_queue_size,
            "queue_capacity": 5,
            "events": events,
            "layout": self.database.get_layout(train_no),
        }
