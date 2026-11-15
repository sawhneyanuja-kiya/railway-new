// A mutual-exclusion lock for async code. Callers queue up, and each one runs
// its critical section only after the previous holder has released the lock.
export class Mutex {
  private tail: Promise<void> = Promise.resolve();
  private held = false;

  isLocked(): boolean {
    return this.held;
  }

  async runExclusive<T>(criticalSection: () => Promise<T>): Promise<T> {
    const previous = this.tail;
    let release!: () => void;
    this.tail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    this.held = true;
    try {
      return await criticalSection();
    } finally {
      this.held = false;
      release();
    }
  }
}
