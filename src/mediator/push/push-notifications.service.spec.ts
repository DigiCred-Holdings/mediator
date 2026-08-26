import { jest } from '@jest/globals';
import { PushNotificationsService } from './push-notifications.service';

describe('PushNotificationsService', () => {
  const originalEnv = process.env.SERVICE_ACCOUNT;
  afterEach(() => {
    if (originalEnv === undefined) delete process.env.SERVICE_ACCOUNT;
    else process.env.SERVICE_ACCOUNT = originalEnv;
  });

  it('is a no-op when SERVICE_ACCOUNT is not configured', () => {
    delete process.env.SERVICE_ACCOUNT;
    const service = new PushNotificationsService();

    const registerMessageHandlers = jest.fn();
    const on = jest.fn();
    const agent = {
      config: { logger: { info: () => undefined, debug: () => undefined, error: () => undefined } },
      didcomm: { registerMessageHandlers },
      events: { on },
    } as any;

    expect(() => service.setup(agent)).not.toThrow();
    // Nothing registered, no observer attached.
    expect(registerMessageHandlers).not.toHaveBeenCalled();
    expect(on).not.toHaveBeenCalled();
  });
});
