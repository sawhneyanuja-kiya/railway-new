// In-memory execution log. The booking endpoints call addEvent() as they run,
// and the browser polls /api/log to print each new line in the Execution Log box.

export type LogEvent = {
  id: number;
  time: string;
  message: string;
};

const events: LogEvent[] = [];
// Ids keep increasing even after clear(), so the browser never misses or repeats a line.
let nextId = 1;

function formatTime(date: Date): string {
  const hms = date.toTimeString().slice(0, 8);
  const ms = String(date.getMilliseconds()).padStart(3, "0");
  return `${hms}.${ms}`;
}

export function addEvent(message: string): void {
  events.push({ id: nextId++, time: formatTime(new Date()), message });
}

export function eventsSince(lastId: number): LogEvent[] {
  return events.filter((event) => event.id > lastId);
}

export function clearEvents(): void {
  events.length = 0;
}
