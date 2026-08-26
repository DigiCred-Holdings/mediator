import * as admin from 'firebase-admin';
import type { Agent } from '@credo-ts/core';
import {
  DidCommConnectionRepository,
  DidCommEventTypes,
  DidCommForwardMessage,
  DidCommMediationRepository,
  DidCommMessageProcessedEvent,
} from '@credo-ts/didcomm';
import type { DidCommMessageHandler, DidCommInboundMessageContext } from '@credo-ts/didcomm';
import {
  PushNotificationsApnsSetDeviceInfoMessage,
  PushNotificationsFcmSetDeviceInfoMessage,
} from './push-notification-messages';

const PUSH_METADATA_KEY = 'pushNotificationMetadata';

type PushMetadata = {
  deviceToken: string;
  devicePlatform: 'ios' | 'android';
};

/**
 * Push notifications for offline recipients, ported from the credo-mediator
 * repo to the Credo 0.7 API. Wallets register their device token over DIDComm
 * (set-device-info); when a message is forwarded for a recipient, the mediator
 * sends an FCM/APNs push so the wallet wakes up and picks it up.
 *
 * Disabled (a no-op) unless SERVICE_ACCOUNT (base64 Firebase service-account
 * JSON) is configured, so the mediator runs fine without push.
 */
export class PushNotificationsService {
  private readonly enabled: boolean;
  private app?: admin.app.App;

  public constructor() {
    const serviceAccount = process.env.SERVICE_ACCOUNT;
    this.enabled = Boolean(serviceAccount);
    if (serviceAccount) {
      const json = JSON.parse(Buffer.from(serviceAccount, 'base64').toString('utf8'));
      this.app = admin.apps.length
        ? admin.app()
        : admin.initializeApp({ credential: admin.credential.cert(json) });
    }
  }

  public setup(agent: Agent): void {
    if (!this.enabled) {
      agent.config.logger.info('Push notifications disabled (SERVICE_ACCOUNT not set)');
      return;
    }

    agent.didcomm.registerMessageHandlers([
      this.fcmHandler(),
      this.apnsHandler(),
    ]);

    agent.events.on(
      DidCommEventTypes.DidCommMessageProcessed,
      async ({ payload }: DidCommMessageProcessedEvent) => {
        if (payload.message.type === DidCommForwardMessage.type.messageTypeUri) {
          await this.onForward(agent, payload.message as DidCommForwardMessage);
        }
      },
    );

    agent.config.logger.info('Push notifications enabled');
  }

  private fcmHandler(): DidCommMessageHandler {
    return {
      supportedMessages: [PushNotificationsFcmSetDeviceInfoMessage],
      handle: async (ctx: DidCommInboundMessageContext<PushNotificationsFcmSetDeviceInfoMessage>) => {
        const { message } = ctx;
        const platform = message.devicePlatform === 'ios' ? 'ios' : 'android';
        await this.storeDeviceInfo(
          ctx,
          message.deviceToken && message.devicePlatform
            ? { deviceToken: message.deviceToken, devicePlatform: platform }
            : undefined,
        );
        return undefined;
      },
    };
  }

  private apnsHandler(): DidCommMessageHandler {
    return {
      supportedMessages: [PushNotificationsApnsSetDeviceInfoMessage],
      handle: async (ctx: DidCommInboundMessageContext<PushNotificationsApnsSetDeviceInfoMessage>) => {
        const { message } = ctx;
        await this.storeDeviceInfo(
          ctx,
          message.deviceToken ? { deviceToken: message.deviceToken, devicePlatform: 'ios' } : undefined,
        );
        return undefined;
      },
    };
  }

  private async storeDeviceInfo(
    ctx: DidCommInboundMessageContext,
    metadata: PushMetadata | undefined,
  ): Promise<void> {
    const connection = ctx.connection;
    if (!connection) return;
    const repository = ctx.agentContext.dependencyManager.resolve(DidCommConnectionRepository);

    if (metadata) {
      connection.metadata.set(PUSH_METADATA_KEY, metadata);
    } else {
      connection.metadata.delete(PUSH_METADATA_KEY);
    }
    await repository.update(ctx.agentContext, connection);
  }

  private async onForward(agent: Agent, forward: DidCommForwardMessage): Promise<void> {
    const logger = agent.config.logger;
    try {
      const mediationRepository = agent.dependencyManager.resolve(DidCommMediationRepository);
      const mediationRecord = await mediationRepository.getSingleByRecipientKey(
        agent.context,
        forward.to,
      );
      const connection = await agent.didcomm.connections.findById(mediationRecord.connectionId);
      if (!connection) return;

      const metadata = connection.metadata.get(PUSH_METADATA_KEY) as PushMetadata | null;
      if (!metadata) return;

      await this.sendFcm(metadata, {
        title: 'New Message Notification',
        body: `You have a new message${connection.theirLabel ? ` from ${connection.theirLabel}` : ''}`,
      });
      logger.debug(`Push notification sent for connection ${connection.id}`);
    } catch (error) {
      logger.error(`Failed to send push notification: ${(error as Error).message}`);
    }
  }

  private async sendFcm(metadata: PushMetadata, notification: { title: string; body: string }): Promise<void> {
    await admin.messaging().send({
      token: metadata.deviceToken,
      notification,
      apns:
        metadata.devicePlatform === 'ios'
          ? { payload: { aps: { sound: 'default' } } }
          : undefined,
    });
  }

  public async shutdown(): Promise<void> {
    if (this.app) await this.app.delete();
  }
}
