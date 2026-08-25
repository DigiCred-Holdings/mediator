import { AbuseMonitor } from './abuse-monitor';

describe('AbuseMonitor', () => {
  it('allows a connection up to the per-window limit', () => {
    const monitor = new AbuseMonitor({ windowMs: 1000, maxMessagesPerWindow: 5, blockDurationMs: 1000 });
    const now = 10_000;
    for (let i = 0; i < 5; i++) {
      expect(monitor.record('c', now)).toBe(false);
    }
  });

  it('blocks once the per-window limit is exceeded', () => {
    const monitor = new AbuseMonitor({ windowMs: 1000, maxMessagesPerWindow: 5, blockDurationMs: 1000 });
    const now = 10_000;
    for (let i = 0; i < 5; i++) monitor.record('c', now);

    // 6th within the window trips the block.
    expect(monitor.record('c', now)).toBe(true);
    expect(monitor.isBlocked('c', now)).toBe(true);
  });

  it('keeps rejecting while blocked, then unblocks after the cooldown', () => {
    const monitor = new AbuseMonitor({ windowMs: 1000, maxMessagesPerWindow: 2, blockDurationMs: 5000 });
    const t0 = 0;
    monitor.record('c', t0);
    monitor.record('c', t0);
    expect(monitor.record('c', t0)).toBe(true); // blocked at t0

    // Still blocked partway through the cooldown.
    expect(monitor.isBlocked('c', t0 + 4999)).toBe(true);

    // Cooldown elapsed → unblocked, and a fresh forward is allowed.
    expect(monitor.isBlocked('c', t0 + 5000)).toBe(false);
    expect(monitor.record('c', t0 + 5000)).toBe(false);
  });

  it('forgets old hits outside the sliding window', () => {
    const monitor = new AbuseMonitor({ windowMs: 1000, maxMessagesPerWindow: 3, blockDurationMs: 1000 });
    // 3 hits early in the window.
    monitor.record('c', 0);
    monitor.record('c', 100);
    monitor.record('c', 200);

    // Much later, the early hits have aged out — not blocked.
    expect(monitor.record('c', 5000)).toBe(false);
  });

  it('tracks connections independently', () => {
    const monitor = new AbuseMonitor({ windowMs: 1000, maxMessagesPerWindow: 2, blockDurationMs: 1000 });
    const now = 0;
    monitor.record('a', now);
    monitor.record('a', now);
    expect(monitor.record('a', now)).toBe(true); // a blocked

    expect(monitor.isBlocked('b', now)).toBe(false);
    expect(monitor.record('b', now)).toBe(false); // b unaffected
  });

  it('validates configuration', () => {
    expect(() => new AbuseMonitor({ windowMs: 0 })).toThrow();
    expect(() => new AbuseMonitor({ maxMessagesPerWindow: 0 })).toThrow();
    expect(() => new AbuseMonitor({ blockDurationMs: 0 })).toThrow();
  });
});
