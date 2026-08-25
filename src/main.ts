// Must be imported before any @credo-ts package: registers the native askar
// bindings that @credo-ts/askar captures at module load time.
import '@openwallet-foundation/askar-nodejs';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { SwaggerModule, DocumentBuilder } from '@nestjs/swagger';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, {logger: console});

  const config = new DocumentBuilder()
  .setTitle('DigCred Mediator')
  .setDescription('API for controlling the DigiCred Mediator')
  .setVersion('1.0')
  .addTag('mediator')
  .build();
  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup('api-1', app, document);

  await app.listen(process.env.PORT ?? 3000);
}
bootstrap();
