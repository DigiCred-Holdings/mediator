// Must load before any @credo-ts package: registers the native askar bindings.
import '@openwallet-foundation/askar-nodejs';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { logger: console });
  // Bind the control API to localhost only; it is never exposed publicly.
  await app.listen(Number(process.env.PORT ?? 3000), '127.0.0.1');
}
bootstrap();
