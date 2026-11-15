"""SQLite data access and seeded railway directory."""

from __future__ import annotations

from pathlib import Path
import sqlite3
from threading import RLock
from typing import Any


DATABASE_PATH = Path(__file__).with_name("railway.db")

TRAINS = [
    (12951, "Mumbai Rajdhani Express", "Mumbai Central", "New Delhi", "16:35", "08:35"),
    (12002, "Bhopal Shatabdi Express", "New Delhi", "Rani Kamlapati", "06:00", "14:25"),
    (12301, "Howrah Rajdhani Express", "Howrah Junction", "New Delhi", "16:50", "09:55"),
    (22436, "Vande Bharat Express", "New Delhi", "Varanasi Junction", "06:00", "14:00"),
    (12952, "Mumbai Rajdhani Express", "New Delhi", "Mumbai Central", "16:55", "08:35"),
]

COACHES = [
    ("1A", "First AC"),
    ("2A", "AC 2 Tier"),
    ("S1", "Sleeper"),
    ("CC", "AC Chair Car"),
]

BERTH_TYPES = ("Lower", "Middle", "Upper", "Side Lower", "Side Upper", "Lower")

DEMO_BOOKINGS = [
    (12951, "2A", 3, "Riya Mehta"),
    (12951, "S1", 8, "Kabir Shah"),
    (12002, "CC", 2, "Aarav Nair"),
    (12301, "1A", 5, "Ishita Roy"),
    (22436, "CC", 9, "Dev Malhotra"),
]


class RailwayDatabase:
    """A shared SQLite connection safe to call from worker threads."""

    def __init__(self, path: Path = DATABASE_PATH) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = RLock()
        self.connection = sqlite3.connect(
            self.path, timeout=15, check_same_thread=False, isolation_level=None
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 15000")
        self.initialize()

    def initialize(self) -> None:
        with self.lock:
            self.connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS trains (
                    train_no INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    source TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    departure TEXT NOT NULL,
                    arrival TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS coaches (
                    coach_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    train_no INTEGER NOT NULL REFERENCES trains(train_no) ON DELETE CASCADE,
                    code TEXT NOT NULL,
                    class_name TEXT NOT NULL,
                    UNIQUE(train_no, code)
                );
                CREATE TABLE IF NOT EXISTS seats (
                    seat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    coach_id INTEGER NOT NULL REFERENCES coaches(coach_id) ON DELETE CASCADE,
                    seat_no INTEGER NOT NULL,
                    berth_type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'AVAILABLE',
                    passenger TEXT,
                    UNIQUE(coach_id, seat_no)
                );
                CREATE TABLE IF NOT EXISTS bookings (
                    booking_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    seat_id INTEGER NOT NULL REFERENCES seats(seat_id),
                    passenger TEXT NOT NULL,
                    booked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_bookings_seat ON bookings(seat_id);
                """
            )
            train_count = self.connection.execute(
                "SELECT COUNT(*) FROM trains"
            ).fetchone()[0]
            if train_count == 0:
                self._seed_locked()

    def _seed_locked(self) -> None:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self.connection.executemany(
                """INSERT INTO trains
                   (train_no, name, source, destination, departure, arrival)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                TRAINS,
            )
            for train_no, *_ in TRAINS:
                for code, class_name in COACHES:
                    cursor = self.connection.execute(
                        """INSERT INTO coaches (train_no, code, class_name)
                           VALUES (?, ?, ?)""",
                        (train_no, code, class_name),
                    )
                    coach_id = cursor.lastrowid
                    for seat_no in range(1, 13):
                        self.connection.execute(
                            """INSERT INTO seats (coach_id, seat_no, berth_type)
                               VALUES (?, ?, ?)""",
                            (coach_id, seat_no, BERTH_TYPES[(seat_no - 1) % len(BERTH_TYPES)]),
                        )
            for train_no, coach_code, seat_no, passenger in DEMO_BOOKINGS:
                self.connection.execute(
                    """INSERT INTO bookings (seat_id, passenger)
                       SELECT s.seat_id, ?
                       FROM seats AS s JOIN coaches AS c ON c.coach_id = s.coach_id
                       WHERE c.train_no = ? AND c.code = ? AND s.seat_no = ?""",
                    (passenger, train_no, coach_code, seat_no),
                )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def list_trains(self, search: str = "") -> list[dict[str, Any]]:
        query = f"%{search.strip()}%"
        with self.lock:
            rows = self.connection.execute(
                """SELECT t.*,
                          COUNT(DISTINCT s.seat_id) AS seat_count,
                          COUNT(DISTINCT CASE WHEN b.booking_id IS NOT NULL
                              THEN s.seat_id END) AS occupied_seats
                   FROM trains AS t
                   JOIN coaches AS c ON c.train_no = t.train_no
                   JOIN seats AS s ON s.coach_id = c.coach_id
                   LEFT JOIN bookings AS b ON b.seat_id = s.seat_id
                   WHERE (? = '%%' OR CAST(t.train_no AS TEXT) LIKE ?
                          OR t.name LIKE ? OR t.source LIKE ? OR t.destination LIKE ?)
                   GROUP BY t.train_no ORDER BY t.train_no""",
                (query, query, query, query, query),
            ).fetchall()
        return [
            {
                **dict(row),
                "available_seats": row["seat_count"] - row["occupied_seats"],
            }
            for row in rows
        ]

    def get_layout(self, train_no: int) -> dict[str, Any] | None:
        with self.lock:
            train = self.connection.execute(
                "SELECT * FROM trains WHERE train_no = ?", (train_no,)
            ).fetchone()
            if train is None:
                return None
            coaches = self.connection.execute(
                "SELECT * FROM coaches WHERE train_no = ? ORDER BY coach_id",
                (train_no,),
            ).fetchall()
            coach_rows: list[dict[str, Any]] = []
            for coach in coaches:
                seats = self.connection.execute(
                    """SELECT s.seat_id, s.seat_no, s.berth_type
                       FROM seats AS s
                       WHERE s.coach_id = ? ORDER BY s.seat_no""",
                    (coach["coach_id"],),
                ).fetchall()
                bookings = self.connection.execute(
                    """SELECT b.seat_id, b.passenger
                       FROM bookings AS b JOIN seats AS s ON s.seat_id = b.seat_id
                       WHERE s.coach_id = ? ORDER BY b.booking_id""",
                    (coach["coach_id"],),
                ).fetchall()
                by_seat: dict[int, list[str]] = {}
                for booking in bookings:
                    by_seat.setdefault(booking["seat_id"], []).append(booking["passenger"])
                seat_rows = []
                for row in seats:
                    passengers = by_seat.get(row["seat_id"], [])
                    status = (
                        "conflict" if len(passengers) > 1
                        else "booked" if passengers
                        else "available"
                    )
                    seat_rows.append(
                        {
                            "seat_id": row["seat_id"],
                            "seat_no": row["seat_no"],
                            "label": f"{coach['code']}-{row['seat_no']:02d}",
                            "berth_type": row["berth_type"],
                            "status": status,
                            "passengers": passengers,
                        }
                    )
                coach_rows.append(
                    {
                        "coach_id": coach["coach_id"],
                        "code": coach["code"],
                        "class_name": coach["class_name"],
                        "seats": seat_rows,
                    }
                )
        return {"train": dict(train), "coaches": coach_rows}

    def get_seat(self, seat_id: int) -> dict[str, Any] | None:
        with self.lock:
            row = self.connection.execute(
                """SELECT s.seat_id, s.seat_no, s.berth_type, c.code AS coach_code,
                          c.train_no
                   FROM seats AS s JOIN coaches AS c ON c.coach_id = s.coach_id
                   WHERE s.seat_id = ?""",
                (seat_id,),
            ).fetchone()
            if row is None:
                return None
            return dict(row)

    def booking_count(self, seat_id: int) -> int:
        with self.lock:
            row = self.connection.execute(
                "SELECT COUNT(*) FROM bookings WHERE seat_id = ?", (seat_id,)
            ).fetchone()
            return int(row[0])

    def seat_status(self, seat_id: int) -> str:
        with self.lock:
            row = self.connection.execute(
                "SELECT status FROM seats WHERE seat_id = ?", (seat_id,)
            ).fetchone()
            return "BOOKED" if row is None else str(row["status"])

    def insert_booking(self, seat_id: int, passenger: str) -> None:
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                self.connection.execute(
                    "UPDATE seats SET status = 'BOOKED', passenger = ? WHERE seat_id = ?",
                    (passenger, seat_id),
                )
                self.connection.execute(
                    "INSERT INTO bookings (seat_id, passenger) VALUES (?, ?)",
                    (seat_id, passenger),
                )
                self.connection.commit()
            except Exception:
                self.connection.rollback()
                raise

    def book_if_available(self, seat_id: int, passenger: str) -> bool:
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                count = self.connection.execute(
                    "SELECT COUNT(*) FROM bookings WHERE seat_id = ?", (seat_id,)
                ).fetchone()[0]
                if count:
                    self.connection.rollback()
                    return False
                self.connection.execute(
                    "INSERT INTO bookings (seat_id, passenger) VALUES (?, ?)",
                    (seat_id, passenger),
                )
                self.connection.execute(
                    "UPDATE seats SET status = 'BOOKED', passenger = ? WHERE seat_id = ?",
                    (passenger, seat_id),
                )
                self.connection.commit()
                return True
            except Exception:
                self.connection.rollback()
                raise

    def cancel_bookings(self, seat_id: int) -> int:
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                count = self.connection.execute(
                    "SELECT COUNT(*) FROM bookings WHERE seat_id = ?", (seat_id,)
                ).fetchone()[0]
                self.connection.execute(
                    "DELETE FROM bookings WHERE seat_id = ?", (seat_id,)
                )
                self.connection.execute(
                    """UPDATE seats SET status = 'AVAILABLE', passenger = NULL
                       WHERE seat_id = ?""",
                    (seat_id,),
                )
                self.connection.commit()
                return int(count)
            except Exception:
                self.connection.rollback()
                raise

    def reset_demo_data(self) -> None:
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                self.connection.execute("DELETE FROM bookings")
                self.connection.execute(
                    "UPDATE seats SET status = 'AVAILABLE', passenger = NULL"
                )
                for train_no, coach_code, seat_no, passenger in DEMO_BOOKINGS:
                    self.connection.execute(
                        """INSERT INTO bookings (seat_id, passenger)
                           SELECT s.seat_id, ?
                           FROM seats AS s JOIN coaches AS c ON c.coach_id = s.coach_id
                           WHERE c.train_no = ? AND c.code = ? AND s.seat_no = ?""",
                        (passenger, train_no, coach_code, seat_no),
                    )
                self.connection.commit()
            except Exception:
                self.connection.rollback()
                raise

    def count_conflicts(self, seat_ids: list[int]) -> int:
        if not seat_ids:
            return 0
        placeholders = ",".join("?" for _ in seat_ids)
        with self.lock:
            row = self.connection.execute(
                f"""SELECT COUNT(*) FROM (
                       SELECT seat_id FROM bookings WHERE seat_id IN ({placeholders})
                       GROUP BY seat_id HAVING COUNT(*) > 1
                   )""",
                seat_ids,
            ).fetchone()
        return int(row[0])
