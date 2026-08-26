import { JsonTransformer } from '@credo-ts/core';
import {
  PushNotificationsApnsSetDeviceInfoMessage,
  PushNotificationsFcmSetDeviceInfoMessage,
} from './push-notification-messages';

describe('push-notification set-device-info messages', () => {
  it('FCM: serializes to the wire format wallets send', () => {
    const msg = new PushNotificationsFcmSetDeviceInfoMessage({
      id: 'msg-0001',
      deviceToken: 'token-123',
      devicePlatform: 'android',
    });
    const json = JsonTransformer.toJSON(msg);

    expect(json['@type']).toBe('https://didcomm.org/push-notifications-fcm/1.0/set-device-info');
    expect(json['@id']).toBe('msg-0001');
    expect(json.device_token).toBe('token-123');
    expect(json.device_platform).toBe('android');
  });

  it('FCM: parses an incoming message into camelCase fields', () => {
    const parsed = JsonTransformer.fromJSON(
      {
        '@type': 'https://didcomm.org/push-notifications-fcm/1.0/set-device-info',
        '@id': 'msg-0002',
        device_token: 'tok',
        device_platform: 'ios',
      },
      PushNotificationsFcmSetDeviceInfoMessage,
    );
    expect(parsed.deviceToken).toBe('tok');
    expect(parsed.devicePlatform).toBe('ios');
  });

  it('APNs: serializes and parses the device_token', () => {
    const msg = new PushNotificationsApnsSetDeviceInfoMessage({ id: 'msg-0003', deviceToken: 'apns-tok' });
    const json = JsonTransformer.toJSON(msg);
    expect(json['@type']).toBe('https://didcomm.org/push-notifications-apns/1.0/set-device-info');
    expect(json.device_token).toBe('apns-tok');

    const parsed = JsonTransformer.fromJSON(json, PushNotificationsApnsSetDeviceInfoMessage);
    expect(parsed.deviceToken).toBe('apns-tok');
  });
});
