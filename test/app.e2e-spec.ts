import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { AppModule } from './../src/app.module';

describe('AppController (e2e)', () => {
  let app: INestApplication;

  beforeEach(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication();
    await app.init();
  });

  afterEach(async () => {
    await app.close();
  });

  it('/invite (GET) fails while the agent has not been started', () => {
    return request(app.getHttpServer()).get('/invite').expect(500);
  });

  it('/oob-invite (GET) fails while the agent has not been started', () => {
    return request(app.getHttpServer()).get('/oob-invite').expect(500);
  });

  it('/unknown (GET) returns 404', () => {
    return request(app.getHttpServer()).get('/unknown').expect(404);
  });
});
