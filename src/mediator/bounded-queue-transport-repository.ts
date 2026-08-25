import { CredoError, utils } from '@credo-ts/core';
import type { AgentContext } from '@credo-ts/core';
import type {
  DidCommQueueTransportRepository,
  DidCommEncryptedMessage,
} from '@credo-ts/didcomm';

/**
 * Thrown when a message cannot be queued because a limit has been reached.
 *
 * The mediator surfaces this as a delivery failure to the *sender* of the
 * forward message, so an abusive sender is rejected while the mediator and
 * every other tenant's queue stay healthy.
 */
export class MediatorQueueLimitReachedError extends CredoError {}

export interface BoundedQueueTransportRepositoryOptions {
  /**
   * Maximum number of pending messages that may be queued for a single
   * mediated connection. Once reached, further forwards for that connection
   * are rejected until the recipient picks messages up.
   *
   * @default 100
   */
  maxMessagesPerConnection?: number;
  /**
   * Maximum number of pending messages across all connections. Protects the
   * process as a whole from being filled by many connections at once.
   *
   * @default 50000
   */
  maxMessagesTotal?: number;
  /**
   * Optional time-to-live (ms) for a queued message. Messages older than this
   * are evicted lazily on the next queue operation, so a recipient that never
   * comes back online cannot pin memory forever.
   *
   * @default undefined (no expiry)
   */
  messageTtlMs?: number;
}

interface StoredMessage {
  id: string;
  receivedAt: Date;
  connectionId: string;
  encryptedMessage: DidCommEncryptedMessage;
  recipientDids: string[];
  state: 'pending' | 'sending';
}

/**
 * A drop-in replacement for Credo's default `InMemoryQueueTransportRepository`
 * that enforces per-connection and global queue-depth limits plus optional
 * message expiry.
 *
 * The default repository pushes every forwarded message onto an unbounded
 * array, so anyone holding a mediation grant can queue messages for an offline
 * recipient until the process runs out of memory. This repository bounds that
 * growth and rejects overflow instead of accepting it.
 */
export class BoundedQueueTransportRepository implements DidCommQueueTransportRepository {
  private messages: StoredMessage[] = [];
  private readonly maxMessagesPerConnection: number;
  private readonly maxMessagesTotal: number;
  private readonly messageTtlMs?: number;

  public constructor(options: BoundedQueueTransportRepositoryOptions = {}) {
    this.maxMessagesPerConnection = options.maxMessagesPerConnection ?? 100;
    this.maxMessagesTotal = options.maxMessagesTotal ?? 50000;
    this.messageTtlMs = options.messageTtlMs;

    if (this.maxMessagesPerConnection < 1) {
      throw new CredoError('maxMessagesPerConnection must be at least 1');
    }
    if (this.maxMessagesTotal < this.maxMessagesPerConnection) {
      throw new CredoError('maxMessagesTotal must be >= maxMessagesPerConnection');
    }
  }

  private evictExpired(now = Date.now()): void {
    if (this.messageTtlMs === undefined) return;
    const cutoff = now - this.messageTtlMs;
    this.messages = this.messages.filter((msg) => msg.receivedAt.getTime() >= cutoff);
  }

  private pendingCountForConnection(connectionId: string, recipientDid?: string): number {
    return this.messages.filter(
      (msg) =>
        msg.connectionId === connectionId &&
        msg.state === 'pending' &&
        (recipientDid === undefined || msg.recipientDids.includes(recipientDid)),
    ).length;
  }

  public getAvailableMessageCount(
    _agentContext: AgentContext,
    options: { connectionId: string; recipientDid?: string },
  ): number {
    this.evictExpired();
    return this.pendingCountForConnection(options.connectionId, options.recipientDid);
  }

  public takeFromQueue(
    agentContext: AgentContext,
    options: { connectionId: string; recipientDid?: string; limit?: number; deleteMessages?: boolean },
  ): StoredMessage[] {
    this.evictExpired();
    const { connectionId, recipientDid, limit, deleteMessages } = options;

    let messages = this.messages.filter(
      (msg) =>
        msg.connectionId === connectionId &&
        msg.state === 'pending' &&
        (recipientDid === undefined || msg.recipientDids.includes(recipientDid)),
    );

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
    _agentContext: AgentContext,
    options: {
      connectionId: string;
      recipientDids: string[];
      payload: DidCommEncryptedMessage;
      receivedAt?: Date;
    },
  ): string {
    this.evictExpired();
    const { connectionId, recipientDids, payload } = options;

    if (this.messages.length >= this.maxMessagesTotal) {
      throw new MediatorQueueLimitReachedError(
        `Global message queue is full (${this.maxMessagesTotal} messages); rejecting forward`,
      );
    }

    if (this.pendingCountForConnection(connectionId) >= this.maxMessagesPerConnection) {
      throw new MediatorQueueLimitReachedError(
        `Message queue for connection ${connectionId} is full (${this.maxMessagesPerConnection} messages); rejecting forward`,
      );
    }

    const id = utils.uuid();
    this.messages.push({
      id,
      receivedAt: options.receivedAt ?? new Date(),
      connectionId,
      encryptedMessage: payload,
      recipientDids,
      state: 'pending',
    });
    return id;
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
