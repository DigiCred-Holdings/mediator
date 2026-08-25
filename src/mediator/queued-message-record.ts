import { BaseRecord } from '@credo-ts/core';
import type { DidCommEncryptedMessage } from '@credo-ts/didcomm';

export type QueuedMessageState = 'pending' | 'sending';

export interface QueuedMessageRecordProps {
  id: string;
  createdAt?: Date;
  connectionId: string;
  recipientDids: string[];
  encryptedMessage: DidCommEncryptedMessage;
  byteSize: number;
  /** Reception time as epoch ms, used for TTL and drop-oldest ordering. */
  receivedAtMs: number;
  state: QueuedMessageState;
}

type DefaultQueuedMessageTags = {
  connectionId: string;
  state: QueuedMessageState;
  recipientDids: string[];
};

/**
 * A single mediated message waiting in the pickup queue, persisted via the
 * agent's storage service (Askar → Postgres). This is what makes the queue
 * survive a mediator restart, unlike the in-memory queue.
 */
export class QueuedMessageRecord extends BaseRecord<DefaultQueuedMessageTags> {
  public static readonly type = 'MediatorQueuedMessage';
  public readonly type = QueuedMessageRecord.type;
  public static readonly allowCache = false;
  public readonly allowCache = false;

  public connectionId!: string;
  public recipientDids!: string[];
  public encryptedMessage!: DidCommEncryptedMessage;
  public byteSize!: number;
  public receivedAtMs!: number;
  public state!: QueuedMessageState;

  public constructor(props?: QueuedMessageRecordProps) {
    super();
    if (props) {
      this.id = props.id;
      this.createdAt = props.createdAt ?? new Date();
      this.connectionId = props.connectionId;
      this.recipientDids = props.recipientDids;
      this.encryptedMessage = props.encryptedMessage;
      this.byteSize = props.byteSize;
      this.receivedAtMs = props.receivedAtMs;
      this.state = props.state;
    }
  }

  public getTags() {
    return {
      ...this._tags,
      connectionId: this.connectionId,
      state: this.state,
      recipientDids: this.recipientDids,
    };
  }
}
