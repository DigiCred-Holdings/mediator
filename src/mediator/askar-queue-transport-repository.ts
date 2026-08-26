import { CredoError, EventEmitter, InjectionSymbols, Repository, utils } from '@credo-ts/core';
import type { AgentContext, StorageService } from '@credo-ts/core';
import type {
  DidCommQueueTransportRepository,
  DidCommEncryptedMessage,
  QueuedDidCommMessage,
} from '@credo-ts/didcomm';
import { AbuseMonitor, AbuseMonitorOptions, MediatorAbuseDetectedError } from './abuse-monitor';
import { QueuedMessageRecord } from './queued-message-record';

export { MediatorAbuseDetectedError } from './abuse-monitor';
export { MediatorQueueLimitReachedError, ONE_WEEK_MS } from './bounded-queue-transport-repository';

import { MediatorQueueLimitReachedError, ONE_WEEK_MS } from './bounded-queue-transport-repository';

const MB = 1024 * 1024;

export interface AskarQueueTransportRepositoryOptions {
  /** @default 200 */
  maxMessagesPerConnection?: number;
  /** @default 500000 */
  maxMessagesTotal?: number;
  /** @default 20 * 1024 * 1024 (20 MB) */
  maxBytesPerConnection?: number;
  /** @default 5 * 1024 * 1024 (5 MB) */
  maxMessageBytes?: number;
  /** @default ONE_WEEK_MS (7 days); 0 disables TTL eviction */
  messageTtlMs?: number;
  /** Rate-based abuse detection; `false` disables it. */
  abuse?: AbuseMonitorOptions | false;
}

// Persistent queue backed by the agent's storage service (Askar -> Postgres),
// so queued messages survive a restart. Per-connection caps + drop-oldest are
// enforced in memory over that connection's rows; the global cap is
// reject-when-full via a lazily-seeded counter (storage API has no COUNT/sort).
export class AskarQueueTransportRepository implements DidCommQueueTransportRepository {
  private readonly maxMessagesPerConnection: number;
  private readonly maxMessagesTotal: number;
  private readonly maxBytesPerConnection: number;
  private readonly maxMessageBytes: number;
  private readonly messageTtlMs: number;
  private readonly abuseMonitor?: AbuseMonitor;

  // Global pending count, seeded lazily from the store, then maintained.
  private globalPending?: number;
  private globalSeed?: Promise<number>;

  // Serializes writes per connection so concurrent processing can't race the
  // queue. Single-process only; multiple instances would need DB-level locking.
  private readonly connectionLocks = new Map<string, Promise<unknown>>();

  public constructor(options: AskarQueueTransportRepositoryOptions = {}) {
    this.maxMessagesPerConnection = options.maxMessagesPerConnection ?? 200;
    this.maxMessagesTotal = options.maxMessagesTotal ?? 500000;
    this.maxBytesPerConnection = options.maxBytesPerConnection ?? 20 * MB;
    this.maxMessageBytes = options.maxMessageBytes ?? 5 * MB;
    this.messageTtlMs = options.messageTtlMs ?? ONE_WEEK_MS;
    this.abuseMonitor = options.abuse === false ? undefined : new AbuseMonitor(options.abuse);

    if (this.maxMessagesPerConnection < 1) throw new CredoError('maxMessagesPerConnection must be at least 1');
    if (this.maxMessagesTotal < this.maxMessagesPerConnection) {
      throw new CredoError('maxMessagesTotal must be >= maxMessagesPerConnection');
    }
    if (this.maxMessageBytes < 1) throw new CredoError('maxMessageBytes must be at least 1');
    if (this.maxBytesPerConnection < this.maxMessageBytes) {
      throw new CredoError('maxBytesPerConnection must be >= maxMessageBytes');
    }
    if (this.messageTtlMs < 0) throw new CredoError('messageTtlMs must be >= 0');
  }

  private withConnectionLock<T>(connectionId: string, fn: () => Promise<T>): Promise<T> {
    const previous = this.connectionLocks.get(connectionId) ?? Promise.resolve();
    const result = previous.then(fn, fn);
    const tail = result.then(
      () => undefined,
      () => undefined,
    );
    this.connectionLocks.set(connectionId, tail);
    void tail.then(() => {
      if (this.connectionLocks.get(connectionId) === tail) this.connectionLocks.delete(connectionId);
    });
    return result;
  }

  private getRepository(agentContext: AgentContext): Repository<QueuedMessageRecord> {
    const storageService = agentContext.dependencyManager.resolve<StorageService<QueuedMessageRecord>>(
      InjectionSymbols.StorageService,
    );
    const eventEmitter = agentContext.dependencyManager.resolve(EventEmitter);
    return new Repository(QueuedMessageRecord, storageService, eventEmitter);
  }

  /** Seed the global pending counter from the store on first use. */
  private async ensureGlobalCount(
    agentContext: AgentContext,
    repository: Repository<QueuedMessageRecord>,
  ): Promise<void> {
    if (this.globalPending !== undefined) return;
    // Guard against concurrent first-calls both running the seed query.
    if (!this.globalSeed) {
      this.globalSeed = repository
        .findByQuery(agentContext, { state: 'pending' })
        .then((pending) => pending.length);
    }
    this.globalPending = await this.globalSeed;
  }

  /** Delete by id, ignoring the case where a concurrent op already removed it. */
  private async deleteIfPresent(
    agentContext: AgentContext,
    repository: Repository<QueuedMessageRecord>,
    id: string,
  ): Promise<boolean> {
    try {
      await repository.deleteById(agentContext, id);
      this.decrementGlobal();
      return true;
    } catch {
      return false;
    }
  }

  /** Load a connection's pending rows, evicting any that have expired. */
  private async loadPending(
    agentContext: AgentContext,
    repository: Repository<QueuedMessageRecord>,
    connectionId: string,
    recipientDid?: string,
  ): Promise<QueuedMessageRecord[]> {
    const query = recipientDid
      ? { connectionId, state: 'pending' as const, recipientDids: [recipientDid] }
      : { connectionId, state: 'pending' as const };
    let records = await repository.findByQuery(agentContext, query);

    if (this.messageTtlMs > 0) {
      const cutoff = Date.now() - this.messageTtlMs;
      const expired = records.filter((r) => r.receivedAtMs < cutoff);
      for (const record of expired) {
        await this.deleteIfPresent(agentContext, repository, record.id);
      }
      records = records.filter((r) => r.receivedAtMs >= cutoff);
    }
    return records;
  }

  private decrementGlobal(): void {
    if (this.globalPending !== undefined && this.globalPending > 0) this.globalPending -= 1;
  }

  public async getAvailableMessageCount(
    agentContext: AgentContext,
    options: { connectionId: string; recipientDid?: string },
  ): Promise<number> {
    const repository = this.getRepository(agentContext);
    const pending = await this.loadPending(agentContext, repository, options.connectionId, options.recipientDid);
    return pending.length;
  }

  public takeFromQueue(
    agentContext: AgentContext,
    options: { connectionId: string; recipientDid?: string; limit?: number; deleteMessages?: boolean },
  ): Promise<QueuedDidCommMessage[]> {
    const { connectionId, recipientDid, limit, deleteMessages } = options;

    return this.withConnectionLock(connectionId, async () => {
      const repository = this.getRepository(agentContext);
      let records = await this.loadPending(agentContext, repository, connectionId, recipientDid);
      // Oldest first, so pickup order is FIFO.
      records.sort((a, b) => a.receivedAtMs - b.receivedAtMs);
      if (limit !== undefined) records = records.slice(0, limit);

      const taken: QueuedMessageRecord[] = [];
      for (const record of records) {
        if (deleteMessages) {
          if (await this.deleteIfPresent(agentContext, repository, record.id)) taken.push(record);
        } else {
          record.state = 'sending';
          await repository.update(agentContext, record);
          taken.push(record);
        }
      }

      return taken.map((record) => ({
        id: record.id,
        receivedAt: new Date(record.receivedAtMs),
        encryptedMessage: record.encryptedMessage,
      }));
    });
  }

  public async addMessage(
    agentContext: AgentContext,
    options: {
      connectionId: string;
      recipientDids: string[];
      payload: DidCommEncryptedMessage;
      receivedAt?: Date;
    },
  ): Promise<string> {
    const { connectionId, recipientDids, payload } = options;
    const logger = agentContext.config.logger;

    // Checked before the lock so floods are rejected without serializing.
    if (this.abuseMonitor?.record(connectionId)) {
      logger.warn(`Mediator blocked forward from connection ${connectionId}: abusive send rate`);
      throw new MediatorAbuseDetectedError(
        `Connection ${connectionId} is temporarily blocked for an abusive send rate`,
      );
    }

    const byteSize = Buffer.byteLength(JSON.stringify(payload), 'utf8');
    if (byteSize > this.maxMessageBytes) {
      throw new MediatorQueueLimitReachedError(
        `Message of ${byteSize} bytes exceeds the per-message limit of ${this.maxMessageBytes} bytes`,
      );
    }

    return this.withConnectionLock(connectionId, async () => {
      const repository = this.getRepository(agentContext);
      await this.ensureGlobalCount(agentContext, repository);

      if ((this.globalPending ?? 0) >= this.maxMessagesTotal) {
        throw new MediatorQueueLimitReachedError(`Global message queue is full (${this.maxMessagesTotal} messages)`);
      }

      const pending = await this.loadPending(agentContext, repository, connectionId);
      pending.sort((a, b) => a.receivedAtMs - b.receivedAtMs); // oldest first
      let currentCount = pending.length;
      let currentBytes = pending.reduce((total, r) => total + r.byteSize, 0);
      let dropIndex = 0;

      while (
        currentCount >= this.maxMessagesPerConnection ||
        currentBytes + byteSize > this.maxBytesPerConnection
      ) {
        if (dropIndex >= pending.length) break; // nothing left to drop
        const oldest = pending[dropIndex++];
        if (await this.deleteIfPresent(agentContext, repository, oldest.id)) {
          currentCount -= 1;
          currentBytes -= oldest.byteSize;
          logger.warn(`Mediator queue for connection ${connectionId} full; dropped oldest pending message`);
        }
      }

      const id = utils.uuid();
      await repository.save(
        agentContext,
        new QueuedMessageRecord({
          id,
          connectionId,
          recipientDids,
          encryptedMessage: payload,
          byteSize,
          receivedAtMs: (options.receivedAt ?? new Date()).getTime(),
          state: 'pending',
        }),
      );
      if (this.globalPending !== undefined) this.globalPending += 1;

      return id;
    });
  }

  public removeMessages(
    agentContext: AgentContext,
    options: { connectionId: string; messageIds: string[] },
  ): Promise<void> {
    return this.withConnectionLock(options.connectionId, async () => {
      const repository = this.getRepository(agentContext);
      for (const messageId of options.messageIds) {
        // deleteIfPresent tolerates a concurrent removal.
        await this.deleteIfPresent(agentContext, repository, messageId);
      }
    });
  }
}
