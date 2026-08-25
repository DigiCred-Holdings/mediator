import { jest } from '@jest/globals';
import { Test, TestingModule } from '@nestjs/testing';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { CreateAgentDto } from './dto/create-agent.dto';

describe('AppController', () => {
  let appController: AppController;

  const appService = {
    startAgent: jest.fn<(dto: CreateAgentDto) => Promise<string>>().mockResolvedValue('OK'),
    createInvitation: jest.fn<() => Promise<String>>().mockResolvedValue('http://localhost:4000?c_i=abc'),
    createOOBInvitation: jest.fn<() => Promise<String>>().mockResolvedValue('http://localhost:4000?oob=abc'),
  };

  beforeEach(async () => {
    jest.clearAllMocks();
    const app: TestingModule = await Test.createTestingModule({
      controllers: [AppController],
      providers: [{ provide: AppService, useValue: appService }],
    }).compile();

    appController = app.get<AppController>(AppController);
  });

  describe('start', () => {
    it('should start the agent with the given config', async () => {
      const dto = {
        label: 'test-mediator',
        walletId: 'wallet-id',
        walletKey: 'wallet-key',
        endpoint: 'http://localhost:4000',
        port: 4000,
      } as CreateAgentDto;

      await expect(appController.startAgent(dto)).resolves.toBe('OK');
      expect(appService.startAgent).toHaveBeenCalledWith(dto);
    });
  });

  describe('invite', () => {
    it('should return a legacy invitation url', async () => {
      await expect(appController.createInvitation()).resolves.toBe('http://localhost:4000?c_i=abc');
      expect(appService.createInvitation).toHaveBeenCalled();
    });
  });

  describe('oob-invite', () => {
    it('should return an out-of-band invitation url', async () => {
      await expect(appController.createOOBInvitation()).resolves.toBe('http://localhost:4000?oob=abc');
      expect(appService.createOOBInvitation).toHaveBeenCalled();
    });
  });
});
