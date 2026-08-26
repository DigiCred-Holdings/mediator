import { CredoError, utils } from '@credo-ts/core';
import type { AgentContext } from '@credo-ts/core';
import type {
  DidCommQueueTransportRepository,
  DidCommEncryptedMessage,
} from '@credo-ts/didcomm';
import { AbuseMonitor, AbuseMonitorOptions, MediatorAbuseDetectedError } from './abuse-monitor';

export { MediatorAbuseDetectedError } from './abuse-monitor';

export const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;

const MB = 1024 * 1024;

// drop-oldest: evict the oldest pending message to admit the new one.
// reject: refuse the new message with MediatorQueueLimitReachedError.
export type QueueOverflowStrategy = 'drop-oldest' | 'reject';

export class MediatorQueueLimitReachedError extends CredoError {}

export interface BoundedQueueTransportRepositoryOptions {
  /** @default 200 */
  maxMessagesPerConnection?: number;
  /** @default 50000 */
  maxMessagesTotal?: number;
  /** @default 20 * 1024 * 1024 (20 MB) */
  maxBytesPerConnection?: number;
  /** @default 500 * 1024 * 1024 (500 MB) */
  maxBytesTotal?: number;
  /** @default 5 * 1024 * 1024 (5 MB) */
  maxMessageBytes?: number;
  /** @default ONE_WEEK_MS (7 days); 0 disables TTL eviction */
  messageTtlMs?: number;
  /** @default 'drop-oldest' */
  overflowStrategy?: QueueOverflowStrategy;
  /** Rate-based abuse detection; `false` disables it. */
  abuse?: AbuseMonitorOptions | false;
}

interface StoredMessage {
  id: string;
  receivedAt: Date;
  connectionId: string;
  encryptedMessage: DidCommEncryptedMessage;
  recipientDids: string[];
  byteSize: number;
  state: 'pending' | 'sending';
}

const byteSizeOf = (payload: DidCommEncryptedMessage): number =>
  Buffer.byteLength(JSON.stringify(payload), 'utf8');

// Bounded in-memory replacement for Credo's unbounded InMemoryQueueTransportRepository.
export class BoundedQueueTransportRepository implements DidCommQueueTransportRepository {
  private messages: StoredMessage[] = [];
  private readonly maxMessagesPerConnection: number;
  private readonly maxMessagesTotal: number;
  private readonly maxBytesPerConnection: number;
  private readonly maxBytesTotal: number;
  private readonly maxMessageBytes: number;
  private readonly messageTtlMs: number;
  private readonly overflowStrategy: QueueOverflowStrategy;
  private readonly abuseMonitor?: AbuseMonitor;

  public constructor(options: BoundedQueueTransportRepositoryOptions = {}) {
    this.maxMessagesPerConnection = options.maxMessagesPerConnection ?? 200;
    this.maxMessagesTotal = options.maxMessagesTotal ?? 50000;
    this.maxBytesPerConnection = options.maxBytesPerConnection ?? 20 * MB;
    this.maxBytesTotal = options.maxBytesTotal ?? 500 * MB;
    this.maxMessageBytes = options.maxMessageBytes ?? 5 * MB;
    this.messageTtlMs = options.messageTtlMs ?? ONE_WEEK_MS;
    this.overflowStrategy = options.overflowStrategy ?? 'drop-oldest';
    this.abuseMonitor = options.abuse === false ? undefined : new AbuseMonitor(options.abuse);

    if (this.maxMessagesPerConnection < 1) {
      throw new CredoError('maxMessagesPerConnection must be at least 1');
    }
    if (this.maxMessagesTotal < this.maxMessagesPerConnection) {
      throw new CredoError('maxMessagesTotal must be >= maxMessagesPerConnection');
    }
    if (this.maxMessageBytes < 1) {
      throw new CredoError('maxMessageBytes must be at least 1');
    }
    if (this.maxBytesPerConnection < this.maxMessageBytes) {
      throw new CredoError('maxBytesPerConnection must be >= maxMessageBytes');
    }
    if (this.maxBytesTotal < this.maxBytesPerConnection) {
      throw new CredoError('maxBytesTotal must be >= maxBytesPerConnection');
    }
    if (this.messageTtlMs < 0) {
      throw new CredoError('messageTtlMs must be >= 0');
    }
  }

  /** Drop every message older than the configured TTL. */
  private evictExpired(now = Date.now()): void {
    if (this.messageTtlMs === 0) return;
    const cutoff = now - this.messageTtlMs;
    this.messages = this.messages.filter((msg) => msg.receivedAt.getTime() >= cutoff);
  }

  private pendingForConnection(connectionId: string, recipientDid?: string): StoredMessage[] {
    return this.messages.filter(
      (msg) =>
        msg.connectionId === connectionId &&
        msg.state === 'pending' &&
        (recipientDid === undefined || msg.recipientDids.includes(recipientDid)),
    );
  }

  private sumBytes(messages: StoredMessage[]): number {
    return messages.reduce((total, msg) => total + msg.byteSize, 0);
  }

  private dropOldestPending(predicate: (msg: StoredMessage) => boolean): boolean {
    let oldestIndex = -1;
    let oldestTime = Number.POSITIVE_INFINITY;
    for (let i = 0; i < this.messages.length; i++) {
      const msg = this.messages[i];
      if (msg.state === 'pending' && predicate(msg) && msg.receivedAt.getTime() < oldestTime) {
        oldestTime = msg.receivedAt.getTime();
        oldestIndex = i;
      }
    }
    if (oldestIndex === -1) return false;
    this.messages.splice(oldestIndex, 1);
    return true;
  }

  public getAvailableMessageCount(
    _agentContext: AgentContext,
    options: { connectionId: string; recipientDid?: string },
  ): number {
    this.evictExpired();
    return this.pendingForConnection(options.connectionId, options.recipientDid).length;
  }

  public takeFromQueue(
    agentContext: AgentContext,
    options: { connectionId: string; recipientDid?: string; limit?: number; deleteMessages?: boolean },
  ): StoredMessage[] {
    this.evictExpired();
    const { connectionId, recipientDid, limit, deleteMessages } = options;

    let messages = this.pendingForConnection(connectionId, recipientDid);
    const messagesToTake = limit ?? messages.length;
    messages = messages.slice(0, messagesToTake);

    for (const msg of messages) {
      const index = this.messages.findIndex((item) => item.id === msg.id);
      if (index !== -1) this.messages[index].state = 'sending';
    }

    if (deleteMessages) {
      this.removeMessages(agentContext, { connectionId, messageIds: messages.map((msg) => msg.id) });
    }

    return messages;
  }

  public addMessage(
    agentContext: AgentContext,
    options: {
      connectionId: string;
      recipientDids: string[];
      payload: DidCommEncryptedMessage;
      receivedAt?: Date;
    },
  ): string {
    this.evictExpired();
    const { connectionId, recipientDids, payload } = options;
    const logger = agentContext.config.logger;

    if (this.abuseMonitor?.record(connectionId)) {
      logger.warn(`Mediator blocked forward from connection ${connectionId}: abusive send rate`);
      throw new MediatorAbuseDetectedError(
        `Connection ${connectionId} is temporarily blocked for an abusive send rate`,
      );
    }

    const byteSize = byteSizeOf(payload);
    if (byteSize > this.maxMessageBytes) {
      throw new MediatorQueueLimitReachedError(
        `Message of ${byteSize} bytes exceeds the per-message limit of ${this.maxMessageBytes} bytes`,
      );
    }

    this.makeRoom(
      () => this.messages.length >= this.maxMessagesTotal || this.sumBytes(this.messages) + byteSize > this.maxBytesTotal,
      () => true,
      () => logger.warn(`Mediator global queue full; dropped oldest pending message`),
      `Global message queue is full`,
    );

    this.makeRoom(
      () => {
        const pending = this.pendingForConnection(connectionId);
        return pending.length >= this.maxMessagesPerConnection || this.sumBytes(pending) + byteSize > this.maxBytesPerConnection;
      },
      (msg) => msg.connectionId === connectionId,
      () => logger.warn(`Mediator queue for connection ${connectionId} full; dropped oldest pending message`),
      `Message queue for connection ${connectionId} is full`,
    );

    const id = utils.uuid();
    this.messages.push({
      id,
      receivedAt: options.receivedAt ?? new Date(),
      connectionId,
      encryptedMessage: payload,
      recipientDids,
      byteSize,
      state: 'pending',
    });
    return id;
  }

  private makeRoom(
    overLimit: () => boolean,
    dropPredicate: (msg: StoredMessage) => boolean,
    onDrop: () => void,
    rejectMessage: string,
  ): void {
    while (overLimit()) {
      if (this.overflowStrategy === 'reject' || !this.dropOldestPending(dropPredicate)) {
        throw new MediatorQueueLimitReachedError(rejectMessage);
      }
      onDrop();
    }
  }

  public removeMessages(
    _agentContext: AgentContext,
    options: { connectionId: string; messageIds: string[] },
  ): void {
    const { messageIds } = options;
    for (const messageId of messageIds) {
      const index = this.messages.findIndex((item) => item.id === messageId);
      if (index > -1) this.messages.splice(index, 1);
    }
  }
}
