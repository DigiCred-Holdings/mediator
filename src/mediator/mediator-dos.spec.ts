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
} from './bounded-queue-transport-repository';

// The repositories only use agentContext for logging; a minimal stub is enough
// to drive addMessage / takeFromQueue / getAvailableMessageCount.
const agentContext = {
  config: { logger: { debug: () => undefined, error: () => undefined } },
} as unknown as AgentContext;

const encryptedMessage = () =>
  ({ protected: 'e30', iv: 'iv', ciphertext: 'ct', tag: 'tag' }) as any;

const queueN = (
  repo: InMemoryQueueTransportRepository | BoundedQueueTransportRepository,
  connectionId: string,
  count: number,
) => {
  let queued = 0;
  for (let i = 0; i < count; i++) {
    repo.addMessage(agentContext, {
      connectionId,
      recipientDids: [`did:key:recipient-${connectionId}`],
      payload: encryptedMessage(),
    });
    queued++;
  }
  return queued;
};

describe('Mediator queue DoS surface', () => {
  describe('VULNERABILITY: default InMemoryQueueTransportRepository is unbounded', () => {
    it('accepts an unbounded number of messages for a single offline recipient', () => {
      const repo = new InMemoryQueueTransportRepository();

      // Simulate a flood: 25k forwards for one offline connection.
      const flood = 25000;
      queueN(repo, 'victim-connection', flood);

      // Every single message was accepted and is pinned in memory. Nothing in
      // the framework rejected the flood — this is the exhaustion vector.
      expect(
        repo.getAvailableMessageCount(agentContext, { connectionId: 'victim-connection' }),
      ).toBe(flood);
    });

    it('never throws no matter how full the queue gets', () => {
      const repo = new InMemoryQueueTransportRepository();
      expect(() => queueN(repo, 'victim-connection', 100000)).not.toThrow();
    });
  });

  describe('MITIGATION: BoundedQueueTransportRepository enforces limits', () => {
    it('rejects forwards once the per-connection limit is reached', () => {
      const repo = new BoundedQueueTransportRepository({ maxMessagesPerConnection: 100 });

      // First 100 are accepted.
      queueN(repo, 'victim-connection', 100);
      expect(
        repo.getAvailableMessageCount(agentContext, { connectionId: 'victim-connection' }),
      ).toBe(100);

      // The 101st is rejected with a typed error the mediator surfaces to the
      // sender, rather than silently growing.
      expect(() => queueN(repo, 'victim-connection', 1)).toThrow(MediatorQueueLimitReachedError);
    });

    it('isolates tenants: a flooded connection does not block others', () => {
      const repo = new BoundedQueueTransportRepository({ maxMessagesPerConnection: 10 });

      // Attacker fills their own connection's quota.
      queueN(repo, 'attacker-connection', 10);
      expect(() => queueN(repo, 'attacker-connection', 1)).toThrow(MediatorQueueLimitReachedError);

      // An unrelated, honest connection can still receive messages.
      expect(() => queueN(repo, 'honest-connection', 10)).not.toThrow();
      expect(
        repo.getAvailableMessageCount(agentContext, { connectionId: 'honest-connection' }),
      ).toBe(10);
    });

    it('enforces a global cap across all connections', () => {
      const repo = new BoundedQueueTransportRepository({
        maxMessagesPerConnection: 100,
        maxMessagesTotal: 150,
      });

      queueN(repo, 'connection-a', 100);
      queueN(repo, 'connection-b', 50); // total now 150

      // Global cap reached; even a fresh connection under its own per-connection
      // limit is rejected.
      expect(() => queueN(repo, 'connection-c', 1)).toThrow(MediatorQueueLimitReachedError);
    });

    it('frees capacity once messages are picked up', () => {
      const repo = new BoundedQueueTransportRepository({ maxMessagesPerConnection: 5 });

      queueN(repo, 'victim-connection', 5);
      expect(() => queueN(repo, 'victim-connection', 1)).toThrow(MediatorQueueLimitReachedError);

      // Recipient comes online and picks up + deletes messages.
      repo.takeFromQueue(agentContext, { connectionId: 'victim-connection', deleteMessages: true });

      // Queue drained, capacity restored.
      expect(
        repo.getAvailableMessageCount(agentContext, { connectionId: 'victim-connection' }),
      ).toBe(0);
      expect(() => queueN(repo, 'victim-connection', 5)).not.toThrow();
    });

    it('expires messages for recipients that never return (TTL)', () => {
      const repo = new BoundedQueueTransportRepository({
        maxMessagesPerConnection: 100,
        messageTtlMs: 1000,
      });

      // Message received 2s ago (older than the 1s TTL).
      repo.addMessage(agentContext, {
        connectionId: 'victim-connection',
        recipientDids: ['did:key:recipient'],
        payload: encryptedMessage(),
        receivedAt: new Date(Date.now() - 2000),
      });

      // Reading the count triggers lazy eviction of the expired message.
      expect(
        repo.getAvailableMessageCount(agentContext, { connectionId: 'victim-connection' }),
      ).toBe(0);
    });

    it('rejects invalid configuration', () => {
      expect(() => new BoundedQueueTransportRepository({ maxMessagesPerConnection: 0 })).toThrow();
      expect(
        () =>
          new BoundedQueueTransportRepository({
            maxMessagesPerConnection: 100,
            maxMessagesTotal: 10,
          }),
      ).toThrow();
    });
  });
});
