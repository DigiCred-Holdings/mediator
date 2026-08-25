/**
 * Integration tests for the persistent (Askar-backed) pickup queue. These use a
 * real Askar store — an in-memory SQLite store for the bounding tests, and a
 * file-backed store for the persistence-across-restart test — so they prove the
 * queue actually survives in the database, unlike Credo's in-memory default.
 */
import '@openwallet-foundation/askar-nodejs';
import { jest } from '@jest/globals';
import { mkdtempSync, rmSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { Agent, CacheModule, InMemoryLruCache, ConsoleLogger, LogLevel } from '@credo-ts/core';
import type { AgentContext } from '@credo-ts/core';
import { AskarModule, AskarMultiWalletDatabaseScheme } from '@credo-ts/askar';
import { askarNodeJS } from '@openwallet-foundation/askar-nodejs';
import { agentDependencies } from '@credo-ts/node';
import { AskarQueueTransportRepository } from './askar-queue-transport-repository';
import { MediatorQueueLimitReachedError, MediatorAbuseDetectedError } from './askar-queue-transport-repository';

jest.setTimeout(60000);

const STORE_KEY = 'testkey0000000000000000000000000';

const createAgent = async (database: any) => {
  const agent = new Agent({
    config: { logger: new ConsoleLogger(LogLevel.Error) },
    dependencies: agentDependencies,
    modules: {
      askar: new AskarModule({
        askar: askarNodeJS,
        store: { id: 'queue-test', key: STORE_KEY, database },
        multiWalletDatabaseScheme: AskarMultiWalletDatabaseScheme.ProfilePerWallet,
      }),
      cache: new CacheModule({ cache: new InMemoryLruCache({ limit: 100 }) }),
    },
  });
  await agent.initialize();
  return agent;
};

const inMemoryDb = () => ({ type: 'sqlite', config: { inMemory: true } });

const payload = (marker = 'x') =>
  ({ protected: 'e30', iv: 'iv', tag: 'tag', ciphertext: marker }) as any;

const add = (repo: AskarQueueTransportRepository, ctx: AgentContext, connectionId: string, marker = 'x', receivedAt?: Date) =>
  repo.addMessage(ctx, { connectionId, recipientDids: [`did:key:${connectionId}`], payload: payload(marker), receivedAt });

describe('AskarQueueTransportRepository (persistent queue)', () => {
  let agent: Agent;
  let ctx: AgentContext;

  afterEach(async () => {
    if (agent) await agent.shutdown();
  });

  it('stores, counts, and takes messages from the database', async () => {
    agent = await createAgent(inMemoryDb());
    ctx = agent.context;
    const repo = new AskarQueueTransportRepository({ abuse: false });

    await add(repo, ctx, 'conn', 'first');
    await add(repo, ctx, 'conn', 'second');

    expect(await repo.getAvailableMessageCount(ctx, { connectionId: 'conn' })).toBe(2);

    const taken = await repo.takeFromQueue(ctx, { connectionId: 'conn', deleteMessages: true });
    // FIFO order, and the encrypted payload survives the round-trip.
    expect(taken.map((m) => (m.encryptedMessage as any).ciphertext)).toEqual(['first', 'second']);
    expect(await repo.getAvailableMessageCount(ctx, { connectionId: 'conn' })).toBe(0);
  });

  it('caps a connection and drops the oldest message on overflow', async () => {
    agent = await createAgent(inMemoryDb());
    ctx = agent.context;
    const repo = new AskarQueueTransportRepository({ maxMessagesPerConnection: 3, abuse: false });

    const base = Date.now();
    for (let i = 0; i < 5; i++) await add(repo, ctx, 'conn', `m${i}`, new Date(base + i * 1000));

    const taken = await repo.takeFromQueue(ctx, { connectionId: 'conn', deleteMessages: true });
    // Cap 3 → the three newest survive.
    expect(taken.map((m) => (m.encryptedMessage as any).ciphertext)).toEqual(['m2', 'm3', 'm4']);
  });

  it('evicts messages older than the TTL', async () => {
    agent = await createAgent(inMemoryDb());
    ctx = agent.context;
    const repo = new AskarQueueTransportRepository({ messageTtlMs: 1000, abuse: false });

    await add(repo, ctx, 'conn', 'stale', new Date(Date.now() - 5000)); // older than TTL
    await add(repo, ctx, 'conn', 'fresh');

    expect(await repo.getAvailableMessageCount(ctx, { connectionId: 'conn' })).toBe(1);
  });

  it('blocks an abusive connection', async () => {
    agent = await createAgent(inMemoryDb());
    ctx = agent.context;
    const repo = new AskarQueueTransportRepository({
      maxMessagesPerConnection: 100000,
      abuse: { windowMs: 10000, maxMessagesPerWindow: 20, blockDurationMs: 60000 },
    });

    for (let i = 0; i < 20; i++) await add(repo, ctx, 'attacker', `m${i}`);
    await expect(add(repo, ctx, 'attacker', 'over')).rejects.toBeInstanceOf(MediatorAbuseDetectedError);
  });

  it('rejects a single oversize message', async () => {
    agent = await createAgent(inMemoryDb());
    ctx = agent.context;
    const repo = new AskarQueueTransportRepository({ maxMessageBytes: 1024, abuse: false });

    const big = { protected: 'e30', iv: 'iv', tag: 'tag', ciphertext: 'x'.repeat(2048) } as any;
    await expect(
      repo.addMessage(ctx, { connectionId: 'conn', recipientDids: ['did:key:r'], payload: big }),
    ).rejects.toBeInstanceOf(MediatorQueueLimitReachedError);
  });

  it('persists queued messages across a mediator restart', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'mediator-queue-'));
    const database = { type: 'sqlite', config: { path: join(dir, 'wallet.db') } };

    try {
      // First "run": queue two messages, then shut the mediator down.
      agent = await createAgent(database);
      const repo1 = new AskarQueueTransportRepository({ abuse: false });
      await add(repo1, agent.context, 'conn', 'survivor-1');
      await add(repo1, agent.context, 'conn', 'survivor-2');
      await agent.shutdown();

      // Second "run": reopen the same store with a fresh repository instance.
      agent = await createAgent(database);
      const repo2 = new AskarQueueTransportRepository({ abuse: false });

      expect(await repo2.getAvailableMessageCount(agent.context, { connectionId: 'conn' })).toBe(2);
      const taken = await repo2.takeFromQueue(agent.context, { connectionId: 'conn', deleteMessages: true });
      expect(taken.map((m) => (m.encryptedMessage as any).ciphertext).sort()).toEqual([
        'survivor-1',
        'survivor-2',
      ]);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
