/**
 * Denial-of-service / resource-exhaustion tests for the mediator's message
 * queue. These exercise the real Credo 0.7 queue-repository contract, not a
 * mock, so they document the exact behaviour a mediator inherits from the
 * framework and prove that our bounded replacement closes the gap.
 *
 * Threat model: anyone who holds a mediation grant (and with
 * `autoAcceptMediationRequests` that is anyone who has the invitation) can send
 * Forward messages addressed to an offline recipient. Each forward is appended
 * to the recipient's pickup queue. If the queue is unbounded, the sender can
 * exhaust the mediator's memory (default in-memory queue) or storage.
 */
import { AgentContext } from '@credo-ts/core';
import { InMemoryQueueTransportRepository } from '@credo-ts/didcomm';
import {
  BoundedQueueTransportRepository,
  MediatorQueueLimitReachedError,
  MediatorAbuseDetectedError,
  ONE_WEEK_MS,
} from './bounded-queue-transport-repository';

const MB = 1024 * 1024;

// Build a payload whose JSON serialization is approximately `bytes` long.
const payloadOfBytes = (bytes: number) =>
  ({ protected: 'e30', iv: 'iv', tag: 'tag', ciphertext: 'x'.repeat(Math.max(0, bytes - 60)) }) as any;

const addPayload = (
  repo: BoundedQueueTransportRepository,
  connectionId: string,
  payload: any,
) =>
  repo.addMessage(agentContext, {
    connectionId,
    recipientDids: [`did:key:recipient-${connectionId}`],
    payload,
  });

// The repositories only use agentContext for logging; a minimal stub is enough
// to drive addMessage / takeFromQueue / getAvailableMessageCount.
const agentContext = {
  config: {
    logger: { debug: () => undefined, warn: () => undefined, error: () => undefined },
  },
} as unknown as AgentContext;

const encryptedMessage = () =>
  ({ protected: 'e30', iv: 'iv', ciphertext: 'ct', tag: 'tag' }) as any;

const count = (
  repo: InMemoryQueueTransportRepository | BoundedQueueTransportRepository,
  connectionId: string,
): number => repo.getAvailableMessageCount(agentContext, { connectionId }) as number;

const queueN = (
  repo: InMemoryQueueTransportRepository | BoundedQueueTransportRepository,
  connectionId: string,
  n: number,
  receivedAt?: (i: number) => Date,
) => {
  for (let i = 0; i < n; i++) {
    repo.addMessage(agentContext, {
      connectionId,
      recipientDids: [`did:key:recipient-${connectionId}`],
      payload: encryptedMessage(),
      receivedAt: receivedAt?.(i),
    });
  }
};

describe('Mediator queue DoS surface', () => {
  describe('VULNERABILITY: default InMemoryQueueTransportRepository is unbounded', () => {
    it('accepts an unbounded number of messages for a single offline recipient', () => {
      const repo = new InMemoryQueueTransportRepository();
      const flood = 25000;
      queueN(repo, 'victim-connection', flood);
      expect(count(repo, 'victim-connection')).toBe(flood);
    });

    it('never throws no matter how full the queue gets', () => {
      const repo = new InMemoryQueueTransportRepository();
      expect(() => queueN(repo, 'victim-connection', 20000)).not.toThrow();
    });
  });

  describe('MITIGATION (default): bound queue, drop-oldest on overflow', () => {
    it('caps a connection at its limit instead of growing without bound', () => {
      // abuse:false to isolate queue-bounding from rate-based rejection.
      const repo = new BoundedQueueTransportRepository({ maxMessagesPerConnection: 100, abuse: false });

      // Flood far past the cap: the queue stays bounded at the cap.
      queueN(repo, 'victim-connection', 5000);
      expect(count(repo, 'victim-connection')).toBe(100);
    });

    it('keeps the newest messages and drops the oldest (does not reject new traffic)', () => {
      const repo = new BoundedQueueTransportRepository({ maxMessagesPerConnection: 3 });

      // Timestamp each message so we can tell which survived.
      const base = Date.now();
      queueN(repo, 'c', 5, (i) => new Date(base + i * 1000)); // messages 0..4

      const remaining = repo.takeFromQueue(agentContext, { connectionId: 'c' });
      const times = remaining.map((m) => m.receivedAt.getTime() - base).sort((a, b) => a - b);

      // Cap of 3 → the three newest (2s, 3s, 4s) survive; oldest two evicted.
      expect(times).toEqual([2000, 3000, 4000]);
    });

    it('never throws under drop-oldest even when flooded', () => {
      const repo = new BoundedQueueTransportRepository({ maxMessagesPerConnection: 50, abuse: false });
      expect(() => queueN(repo, 'victim-connection', 20000)).not.toThrow();
    });

    it('isolates tenants: a flooded connection does not evict another\'s messages', () => {
      const repo = new BoundedQueueTransportRepository({ maxMessagesPerConnection: 10, abuse: false });

      queueN(repo, 'honest-connection', 10);
      queueN(repo, 'attacker-connection', 20000); // floods only its own queue

      expect(count(repo, 'honest-connection')).toBe(10);
      expect(count(repo, 'attacker-connection')).toBe(10);
    });

    it('enforces a global cap by dropping the oldest pending message anywhere', () => {
      const repo = new BoundedQueueTransportRepository({
        maxMessagesPerConnection: 100,
        maxMessagesTotal: 150,
      });

      queueN(repo, 'connection-a', 100);
      queueN(repo, 'connection-b', 50); // total now 150 (at global cap)

      // A new connection's message is still accepted; total stays capped.
      queueN(repo, 'connection-c', 1);
      const total =
        count(repo, 'connection-a') + count(repo, 'connection-b') + count(repo, 'connection-c');
      expect(total).toBe(150);
      expect(count(repo, 'connection-c')).toBe(1);
    });
  });

  describe('TTL: undelivered messages expire (default one week)', () => {
    it('defaults to a one-week lifetime', () => {
      const repo = new BoundedQueueTransportRepository();
      const base = Date.now();

      repo.addMessage(agentContext, {
        connectionId: 'c',
        recipientDids: ['did:key:r'],
        payload: encryptedMessage(),
        receivedAt: new Date(base - ONE_WEEK_MS - 1000), // just over a week old
      });
      repo.addMessage(agentContext, {
        connectionId: 'c',
        recipientDids: ['did:key:r'],
        payload: encryptedMessage(),
        receivedAt: new Date(base - 60_000), // a minute old
      });

      // The week-old message is evicted lazily; the recent one remains.
      expect(count(repo, 'c')).toBe(1);
    });

    it('lets a stuck queue self-heal once its messages age out', () => {
      const repo = new BoundedQueueTransportRepository({
        maxMessagesPerConnection: 5,
        messageTtlMs: 1000,
      });

      // Fill the queue with messages received 2s ago (older than the 1s TTL).
      queueN(repo, 'c', 5, () => new Date(Date.now() - 2000));

      // On the next operation the stale queue is fully evicted.
      expect(count(repo, 'c')).toBe(0);
    });

    it('can be disabled with messageTtlMs = 0', () => {
      const repo = new BoundedQueueTransportRepository({ messageTtlMs: 0 });
      repo.addMessage(agentContext, {
        connectionId: 'c',
        recipientDids: ['did:key:r'],
        payload: encryptedMessage(),
        receivedAt: new Date(Date.now() - 10 * ONE_WEEK_MS),
      });
      expect(count(repo, 'c')).toBe(1);
    });
  });

  describe('byte caps: a few large messages cannot exhaust memory', () => {
    it('rejects a single message larger than the per-message limit', () => {
      const repo = new BoundedQueueTransportRepository({ maxMessageBytes: 1 * MB });
      expect(() => addPayload(repo, 'c', payloadOfBytes(2 * MB))).toThrow(
        MediatorQueueLimitReachedError,
      );
    });

    it('bounds a connection by total bytes even when the message count is low', () => {
      // 10MB per-connection byte cap, generous count cap. Each message ~4MB.
      const repo = new BoundedQueueTransportRepository({
        maxMessagesPerConnection: 1000,
        maxBytesPerConnection: 10 * MB,
        maxMessageBytes: 5 * MB,
      });

      addPayload(repo, 'c', payloadOfBytes(4 * MB));
      addPayload(repo, 'c', payloadOfBytes(4 * MB)); // 8MB queued
      addPayload(repo, 'c', payloadOfBytes(4 * MB)); // would be 12MB → drop-oldest

      // Byte cap held: only two ~4MB messages fit under 10MB.
      expect(count(repo, 'c')).toBe(2);
    });

    it('enforces the per-message cap under the reject strategy too', () => {
      const repo = new BoundedQueueTransportRepository({
        maxMessageBytes: 1 * MB,
        overflowStrategy: 'reject',
      });
      expect(() => addPayload(repo, 'c', payloadOfBytes(2 * MB))).toThrow(
        MediatorQueueLimitReachedError,
      );
    });
  });

  describe('abuse detection: fast flooders are blocked and rejected', () => {
    it('blocks a connection that exceeds the rate limit and rejects further forwards', () => {
      const repo = new BoundedQueueTransportRepository({
        // Big caps so the queue itself is not the limit — only the rate is.
        maxMessagesPerConnection: 100000,
        maxMessagesTotal: 200000,
        abuse: { windowMs: 10000, maxMessagesPerWindow: 500, blockDurationMs: 60000 },
      });

      // 500 forwards inside the window are allowed.
      queueN(repo, 'attacker', 500);

      // The 501st trips the abuse block; subsequent forwards are rejected.
      expect(() => queueN(repo, 'attacker', 1)).toThrow(MediatorAbuseDetectedError);
      expect(() => queueN(repo, 'attacker', 1)).toThrow(MediatorAbuseDetectedError);
    });

    it('does not block connections sending at a normal rate', () => {
      const repo = new BoundedQueueTransportRepository({
        abuse: { windowMs: 10000, maxMessagesPerWindow: 500, blockDurationMs: 60000 },
      });
      expect(() => queueN(repo, 'honest', 100)).not.toThrow();
    });

    it('blocks only the abusive connection, not others', () => {
      const repo = new BoundedQueueTransportRepository({
        maxMessagesPerConnection: 100000,
        maxMessagesTotal: 200000,
        abuse: { windowMs: 10000, maxMessagesPerWindow: 200, blockDurationMs: 60000 },
      });

      queueN(repo, 'attacker', 200);
      expect(() => queueN(repo, 'attacker', 1)).toThrow(MediatorAbuseDetectedError);

      // An honest connection is unaffected by the attacker's block.
      expect(() => queueN(repo, 'honest', 100)).not.toThrow();
    });

    it('can be disabled with abuse: false', () => {
      const repo = new BoundedQueueTransportRepository({
        maxMessagesPerConnection: 100000,
        maxMessagesTotal: 200000,
        abuse: false,
      });
      expect(() => queueN(repo, 'fast', 5000)).not.toThrow();
    });
  });

  describe('overflowStrategy: reject (opt-in)', () => {
    it('rejects new forwards once the per-connection limit is reached', () => {
      const repo = new BoundedQueueTransportRepository({
        maxMessagesPerConnection: 100,
        overflowStrategy: 'reject',
      });
      queueN(repo, 'victim-connection', 100);
      expect(() => queueN(repo, 'victim-connection', 1)).toThrow(MediatorQueueLimitReachedError);
    });
  });

  describe('configuration validation', () => {
    it('rejects invalid configuration', () => {
      expect(() => new BoundedQueueTransportRepository({ maxMessagesPerConnection: 0 })).toThrow();
      expect(
        () =>
          new BoundedQueueTransportRepository({
            maxMessagesPerConnection: 100,
            maxMessagesTotal: 10,
          }),
      ).toThrow();
      expect(() => new BoundedQueueTransportRepository({ messageTtlMs: -1 })).toThrow();
    });
  });
});
