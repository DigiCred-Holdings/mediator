import { CredoError } from '@credo-ts/core';

/**
 * Thrown when a connection is temporarily blocked for an abusive send rate.
 */
export class MediatorAbuseDetectedError extends CredoError {}

export interface AbuseMonitorOptions {
  /**
   * Sliding window (ms) over which forward rate is measured.
   *
   * @default 10000 (10s)
   */
  windowMs?: number;
  /**
   * Maximum forwards a single connection may send within `windowMs` before it
   * is flagged as abusive and blocked.
   *
   * @default 500
   */
  maxMessagesPerWindow?: number;
  /**
   * How long (ms) a flagged connection stays blocked. While blocked, its
   * forwards are rejected outright.
   *
   * @default 60000 (60s)
   */
  blockDurationMs?: number;
}

/**
 * Per-connection rate-based abuse detector.
 *
 * A connection that exceeds `maxMessagesPerWindow` forwards within `windowMs`
 * is flagged and blocked for `blockDurationMs`. While blocked, its forwards are
 * rejected without touching the queue, so a fast flood is turned away cheaply
 * instead of being processed (and drop-oldest-churned) message by message.
 */
export class AbuseMonitor {
  private readonly windowMs: number;
  private readonly maxMessagesPerWindow: number;
  private readonly blockDurationMs: number;

  private readonly hits = new Map<string, number[]>();
  private readonly blockedUntil = new Map<string, number>();

  public constructor(options: AbuseMonitorOptions = {}) {
    this.windowMs = options.windowMs ?? 10_000;
    this.maxMessagesPerWindow = options.maxMessagesPerWindow ?? 500;
    this.blockDurationMs = options.blockDurationMs ?? 60_000;

    if (this.windowMs < 1) throw new CredoError('windowMs must be at least 1');
    if (this.maxMessagesPerWindow < 1) throw new CredoError('maxMessagesPerWindow must be at least 1');
    if (this.blockDurationMs < 1) throw new CredoError('blockDurationMs must be at least 1');
  }

  /**
   * Whether the connection is currently blocked. Lapsed blocks are cleared as a
   * side effect.
   */
  public isBlocked(connectionId: string, now = Date.now()): boolean {
    const until = this.blockedUntil.get(connectionId);
    if (until === undefined) return false;
    if (now >= until) {
      this.blockedUntil.delete(connectionId);
      this.hits.delete(connectionId);
      return false;
    }
    return true;
  }

  /**
   * Record a forward from `connectionId`. Returns `true` if the connection is
   * (now) blocked and the forward should be rejected.
   */
  public record(connectionId: string, now = Date.now()): boolean {
    if (this.isBlocked(connectionId, now)) return true;

    const cutoff = now - this.windowMs;
    const recent = (this.hits.get(connectionId) ?? []).filter((t) => t >= cutoff);
    recent.push(now);

    if (recent.length > this.maxMessagesPerWindow) {
      // Trip the block; drop the per-hit history to free memory.
      this.blockedUntil.set(connectionId, now + this.blockDurationMs);
      this.hits.delete(connectionId);
      return true;
    }

    this.hits.set(connectionId, recent);
    return false;
  }

  /** Connections currently blocked (for logging / observability). */
  public blockedConnections(now = Date.now()): string[] {
    const blocked: string[] = [];
    for (const [connectionId] of this.blockedUntil) {
      if (this.isBlocked(connectionId, now)) blocked.push(connectionId);
    }
    return blocked;
  }
}
