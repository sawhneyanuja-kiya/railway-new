import tempfile
import unittest
from pathlib import Path
from threading import Barrier, Thread

from booking import BookingSimulator
from railway_db import RailwayDatabase


class RailwayBookingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = RailwayDatabase(Path(self.temp_dir.name) / "railway-test.db")

    def tearDown(self) -> None:
        self.database.connection.close()
        self.temp_dir.cleanup()

    def available_seat(self, train_no: int = 12951) -> int:
        layout = self.database.get_layout(train_no)
        assert layout is not None
        return next(
            seat["seat_id"]
            for coach in layout["coaches"]
            for seat in coach["seats"]
            if seat["status"] == "available"
        )

    def test_seed_contains_directory_coaches_berths_and_demo_bookings(self) -> None:
        trains = self.database.list_trains()

        self.assertEqual(len(trains), 5)
        self.assertEqual(sum(train["available_seats"] for train in trains), 235)
        layout = self.database.get_layout(12951)
        self.assertIsNotNone(layout)
        self.assertEqual([coach["code"] for coach in layout["coaches"]], ["1A", "2A", "S1", "CC"])
        self.assertEqual(len(layout["coaches"][0]["seats"]), 12)
        self.assertEqual(layout["coaches"][1]["seats"][2]["passengers"], ["Riya Mehta"])

    def test_normal_booking_is_atomic_across_threads(self) -> None:
        seat_id = self.available_seat()
        barrier = Barrier(12)
        outcomes: list[bool] = []

        def book(number: int) -> None:
            barrier.wait()
            outcomes.append(self.database.book_if_available(seat_id, f"Rider {number}"))

        threads = [Thread(target=book, args=(number,)) for number in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(sum(outcomes), 1)

    def test_book_and_cancel_emit_mutex_lifecycle_events(self) -> None:
        seat_id = self.available_seat()
        seat = self.database.get_seat(seat_id)
        assert seat is not None
        seat_label = f"{seat['coach_code']}-{seat['seat_no']:02d}"
        simulator = BookingSimulator(self.database)
        events: list[tuple[str, str]] = []
        emit = lambda message, kind: events.append((message, kind))

        self.assertTrue(simulator.book_one(seat_id, "Test Passenger", seat_label, emit))
        self.assertIn((f"Attempting to acquire Mutex lock for {seat_label}...", "waiting"), events)
        self.assertIn((f"Lock acquired. Checking {seat_label}...", "entered"), events)
        self.assertIn(("Lock released.", "info"), events)
        self.assertEqual(simulator.cancel_one(seat_id, seat_label, emit), 1)
        self.assertEqual(self.database.seat_status(seat_id), "AVAILABLE")

    def test_unsafe_and_mutex_modes_show_expected_conflicts(self) -> None:
        simulator = BookingSimulator(self.database, io_delay=0.005)
        unsafe_seat = self.available_seat()
        unsafe = simulator.run(12951, [unsafe_seat], 15, "unsafe")
        self.assertEqual((unsafe["confirmed"], unsafe["conflicts"]), (15, 1))
        target_seats = [
            seat
            for coach in unsafe["layout"]["coaches"]
            for seat in coach["seats"]
            if seat["seat_id"] == unsafe_seat
        ]
        self.assertEqual(len(target_seats), 1)
        self.assertEqual(len(target_seats[0]["passengers"]), 15)

        self.database.reset_demo_data()
        mutex_seat = self.available_seat()
        safe = simulator.run(12951, [mutex_seat], 15, "mutex")
        self.assertEqual((safe["confirmed"], safe["rejected"], safe["conflicts"]), (1, 14, 0))

    def test_semaphore_mode_uses_a_bounded_queue_and_single_ticket(self) -> None:
        seat_id = self.available_seat()
        result = BookingSimulator(self.database, io_delay=0.01).run(
            12951, [seat_id], 15, "semaphore"
        )

        self.assertEqual((result["confirmed"], result["rejected"], result["conflicts"]), (1, 14, 0))
        self.assertLessEqual(result["queue_max_size"], result["queue_capacity"])
        self.assertIn("queued", {event["kind"] for event in result["events"]})


if __name__ == "__main__":
    unittest.main()
