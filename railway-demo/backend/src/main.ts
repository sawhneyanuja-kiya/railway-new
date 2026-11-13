/*
 * Railway Reservation System: end-semester viva demo.
 * Focus: Process Synchronization (Mutex / Locks) in a concurrent booking system.
 *
 * 1. CRITICAL SECTION: read seats -> decide -> write seats on a shared row.
 * 2. RACE CONDITION (/api/demo_book_without_sync): two requests read the same
 *    seat count, both pass the check, and both write, so two users get one seat.
 * 3. MUTEX (/api/demo_book_with_sync): only one request at a time may run the
 *    critical section, so the second request reads 0 seats and is rejected.
 *
 * Node runs JavaScript on one thread, but it switches between requests at every
 * `await` (database call or timer), which is enough for the race to happen.
 */
import { fileURLToPath } from "node:url";
import express, { type Request, type Response } from "express";
import { listTrains, readSeats, writeSeats } from "./db.js";
import { addEvent, clearEvents, eventsSince } from "./eventLog.js";
import { Mutex } from "./mutex.js";

const FRONTEND_FILE = fileURLToPath(new URL("../../frontend/index.html", import.meta.url));
const PORT = Number(process.env.PORT ?? 8000);

// The train used for the viva demo. It always has exactly 1 seat.
const DEMO_TRAIN_ID = 1;
// Simulated processing time between "read seats" and "write seats".
const PROCESSING_MS = 2000;

// The single mutex for the whole server process, shared by every sync request.
const bookingLock = new Mutex();

type BookingResult = { success: boolean; user: string; message: string };

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function isValidText(value: unknown): value is string {
  return typeof value === "string" && value.length >= 1 && value.length <= 30;
}

function parseBooking(body: unknown): { user: string; trainId: number } | null {
  const { user, train_id: trainId = DEMO_TRAIN_ID } = (body ?? {}) as Record<string, unknown>;
  if (!isValidText(user) || typeof trainId !== "number" || !Number.isInteger(trainId)) {
    return null;
  }
  return { user, trainId };
}

function trainIdFromQuery(value: unknown): number | null {
  const trainId = value === undefined ? DEMO_TRAIN_ID : Number(value);
  return Number.isInteger(trainId) ? trainId : null;
}

function sendInvalid(res: Response): void {
  res.status(422).json({ error: "invalid request" });
}

const app = express();
app.use(express.json());

// ---------------------------------------------------------------------------
// Static + read-only endpoints
// ---------------------------------------------------------------------------
app.get("/", (_req: Request, res: Response) => {
  res.sendFile(FRONTEND_FILE);
});

app.get("/api/trains", async (_req: Request, res: Response) => {
  res.json({ trains: await listTrains() });
});

app.get("/api/seats", async (req: Request, res: Response) => {
  const trainId = trainIdFromQuery(req.query.train_id);
  if (trainId === null) return sendInvalid(res);
  const seats = await readSeats(trainId);
  if (seats === null) return res.json({ error: "train not found" });
  res.json({ train_id: trainId, available_seats: seats });
});

app.get("/api/log", (req: Request, res: Response) => {
  const since = Number(req.query.since ?? 0);
  res.json({ events: eventsSince(Number.isFinite(since) ? since : 0) });
});

// ---------------------------------------------------------------------------
// Tab 2: User Profile (dummy login, NO real authentication)
// ---------------------------------------------------------------------------
app.post("/api/login", (req: Request, res: Response) => {
  const { username, password } = (req.body ?? {}) as Record<string, unknown>;
  if (!isValidText(username) || !isValidText(password)) return sendInvalid(res);
  // Mock login for the viva UI only: any username/password is accepted.
  const name = username.charAt(0).toUpperCase() + username.slice(1).toLowerCase();
  res.json({
    success: true,
    profile: {
      name,
      age: 21,
      email: `${username.toLowerCase()}@demo.edu`,
      past_bookings: [
        { train: "Express 101", date: "2026-08-12", status: "Completed" },
        { train: "Local 202", date: "2026-09-03", status: "Completed" },
      ],
    },
  });
});

// ---------------------------------------------------------------------------
// Tab 1: Passenger Booking (normal, single-user booking)
// ---------------------------------------------------------------------------
app.post("/api/book_normal", async (req: Request, res: Response) => {
  const booking = parseBooking(req.body);
  if (!booking) return sendInvalid(res);
  const { user, trainId } = booking;
  addEvent(`${user} submitted a normal booking for train ${trainId}`);

  const seats = await readSeats(trainId);
  if (seats === null) return res.json({ success: false, message: "Train not found" });
  if (seats <= 0) return res.json({ success: false, message: "No seats available" });

  await writeSeats(trainId, seats - 1);
  addEvent(`${user} booked 1 seat on train ${trainId} (now ${seats - 1} left)`);
  res.json({ success: true, message: `Seat booked on train ${trainId}` });
});

// ---------------------------------------------------------------------------
// Tab 3 - Panel B: Race Condition (NO lock) -> double-booking
// ---------------------------------------------------------------------------
app.post("/api/demo_book_without_sync", async (req: Request, res: Response) => {
  const booking = parseBooking(req.body);
  if (!booking) return sendInvalid(res);
  const { user, trainId } = booking;
  addEvent(`${user} trying to book train ${trainId}... (NO LOCK)`);

  // RACE CONDITION: this read is NOT protected.
  const seats = await readSeats(trainId);
  if (seats === null) {
    return res.json({ success: false, user, message: "Train not found" } satisfies BookingResult);
  }
  addEvent(`${user} read ${seats} seat(s) from the database`);

  if (seats <= 0) {
    addEvent(`${user} rejected: ${seats} seats left`);
    return res.json({ success: false, user, message: "No seats available" } satisfies BookingResult);
  }

  // THE GAP: the other request runs now and also reads the same seat count.
  addEvent(`${user} simulating payment processing (${PROCESSING_MS / 1000}s)...`);
  await sleep(PROCESSING_MS);

  // Write a value computed from the OLD read: the second write overwrites the first.
  await writeSeats(trainId, seats - 1);
  addEvent(`${user} wrote ${seats - 1} to the database (DOUBLE-BOOKED!)`);
  res.json({ success: true, user, message: "Booked (no lock)" } satisfies BookingResult);
});

// ---------------------------------------------------------------------------
// Tab 3 - Panel B: Synchronized Booking (WITH mutex) -> safe
// ---------------------------------------------------------------------------
app.post("/api/demo_book_with_sync", async (req: Request, res: Response) => {
  const booking = parseBooking(req.body);
  if (!booking) return sendInvalid(res);
  const { user, trainId } = booking;
  addEvent(`${user} trying to book train ${trainId}... (WITH LOCK)`);

  if (bookingLock.isLocked()) {
    addEvent(`${user} is BLOCKED waiting for the mutex...`);
  }

  const result = await bookingLock.runExclusive(async (): Promise<BookingResult> => {
    addEvent(`${user} acquired the mutex (entered critical section)`);
    try {
      const seats = await readSeats(trainId);
      if (seats === null) return { success: false, user, message: "Train not found" };
      addEvent(`${user} read ${seats} seat(s) from the database`);

      if (seats <= 0) {
        addEvent(`${user} rejected: ${seats} seats left (under lock)`);
        return { success: false, user, message: "No seats available" };
      }

      addEvent(`${user} simulating payment processing (${PROCESSING_MS / 1000}s, holding lock)...`);
      await sleep(PROCESSING_MS);

      await writeSeats(trainId, seats - 1);
      addEvent(`${user} wrote ${seats - 1} to the database`);
      return { success: true, user, message: "Booked (mutex protected)" };
    } finally {
      addEvent(`${user} released the mutex`);
    }
  });
  res.json(result);
});

// ---------------------------------------------------------------------------
// Tab 3 - Panel A: Admin reset of the demo train
// ---------------------------------------------------------------------------
app.post("/api/reset", async (req: Request, res: Response) => {
  const trainId = trainIdFromQuery(req.query.train_id);
  if (trainId === null) return sendInvalid(res);
  await writeSeats(trainId, 1);
  clearEvents();
  addEvent(`Admin reset train ${trainId} to 1 seat available`);
  res.json({ train_id: trainId, available_seats: 1 });
});

app.listen(PORT, () => {
  console.log(`Railway demo backend listening on http://localhost:${PORT}`);
});
