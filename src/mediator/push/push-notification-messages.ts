import { DidCommMessage, IsValidMessageType, parseMessageType } from '@credo-ts/didcomm';
import { Expose } from 'class-transformer';
import { IsString, ValidateIf } from 'class-validator';

// Native Credo 0.7 reimplementation of the push-notifications set-device-info
// messages (the @credo-ts/push-notifications package is Credo 0.5 only). Wire
// format matches: type URIs and `device_token` / `device_platform` attributes.

export interface FcmSetDeviceInfoOptions {
  id?: string;
  deviceToken: string | null;
  devicePlatform: string | null;
}

export class PushNotificationsFcmSetDeviceInfoMessage extends DidCommMessage {
  public static readonly type = parseMessageType(
    'https://didcomm.org/push-notifications-fcm/1.0/set-device-info',
  );

  @IsValidMessageType(PushNotificationsFcmSetDeviceInfoMessage.type)
  public readonly type = PushNotificationsFcmSetDeviceInfoMessage.type.messageTypeUri;

  @Expose({ name: 'device_token' })
  @IsString()
  @ValidateIf((_, value) => value !== null)
  public deviceToken!: string | null;

  @Expose({ name: 'device_platform' })
  @IsString()
  @ValidateIf((_, value) => value !== null)
  public devicePlatform!: string | null;

  public constructor(options?: FcmSetDeviceInfoOptions) {
    super();
    if (options) {
      this.id = options.id ?? this.generateId();
      this.deviceToken = options.deviceToken;
      this.devicePlatform = options.devicePlatform;
    }
  }
}

export interface ApnsSetDeviceInfoOptions {
  id?: string;
  deviceToken: string | null;
}

export class PushNotificationsApnsSetDeviceInfoMessage extends DidCommMessage {
  public static readonly type = parseMessageType(
    'https://didcomm.org/push-notifications-apns/1.0/set-device-info',
  );

  @IsValidMessageType(PushNotificationsApnsSetDeviceInfoMessage.type)
  public readonly type = PushNotificationsApnsSetDeviceInfoMessage.type.messageTypeUri;

  @Expose({ name: 'device_token' })
  @IsString()
  @ValidateIf((_, value) => value !== null)
  public deviceToken!: string | null;

  public constructor(options?: ApnsSetDeviceInfoOptions) {
    super();
    if (options) {
      this.id = options.id ?? this.generateId();
      this.deviceToken = options.deviceToken;
    }
  }
}
