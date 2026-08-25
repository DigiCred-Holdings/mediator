// Keep this import first so the native askar bindings are registered before
// any @credo-ts package loads (they capture the binding at module load time).
import { askarNodeJS } from '@openwallet-foundation/askar-nodejs';
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { CreateAgentDto } from './dto/create-agent.dto';
import {
  Agent,
  CacheModule,
  InMemoryLruCache,
  InitConfig,
  ConsoleLogger,
  LogLevel,
} from '@credo-ts/core';
import {
  DidCommModule,
  DidCommHttpOutboundTransport,
  DidCommWsOutboundTransport,
  DidCommProofEventTypes,
  DidCommProofStateChangedEvent,
  DidCommBasicMessageRole,
  DidCommBasicMessageEventTypes,
  DidCommBasicMessageStateChangedEvent,
  DidCommConnectionEventTypes,
  DidCommConnectionStateChangedEvent,
  DidCommDidExchangeState,
} from '@credo-ts/didcomm';
import { AskarModule, AskarMultiWalletDatabaseScheme } from '@credo-ts/askar';
import { WebSocketServer } from 'ws';
import { DidCommHttpInboundTransport, DidCommWsInboundTransport, agentDependencies } from '@credo-ts/node';
import type { Socket } from 'net';
import { askarPostgresConfig } from './database';

const getAgentModules = (createAgentDto: CreateAgentDto, storageConfig: ReturnType<typeof askarPostgresConfig>) => ({
  askar: new AskarModule({
    askar: askarNodeJS,
    store: {
      id: createAgentDto.walletId,
      key: createAgentDto.walletKey,
      database: storageConfig,
    },
    multiWalletDatabaseScheme: AskarMultiWalletDatabaseScheme.ProfilePerWallet,
  }),
  didcomm: new DidCommModule({
    endpoints: [createAgentDto.endpoint],
    mediator: {
      autoAcceptMediationRequests: true,
    },
    connections: {
      autoAcceptConnections: true,
    },
  }),
  cache: new CacheModule({
    cache: new InMemoryLruCache({ limit: 500 }),
  }),
});

type MediatorAgent = Agent<ReturnType<typeof getAgentModules>>;

@Injectable()
export class AppService {

  agentConfig: InitConfig;
  agent: MediatorAgent;
  label: string;
  socketServer: WebSocketServer;

  constructor(private configService: ConfigService) {}

  async startAgent(createAgentDto: CreateAgentDto): Promise<string> {
      console.log("Agent DTO=", createAgentDto)

      // Create PostgreSQL storage configuration
      const storageConfig = askarPostgresConfig(this.configService);

      this.label = createAgentDto.label;

      // Setup the configuration for the agent
      this.agentConfig = {
        logger: new ConsoleLogger(LogLevel.Info)
      }

      // Set up agent
      this.agent = new Agent({
        config: this.agentConfig,
        dependencies: agentDependencies,
        modules: getAgentModules(createAgentDto, storageConfig),
      });

      // Initialize websocket server
      this.socketServer = new WebSocketServer({ noServer: true });
      console.log("Created socketServer");

      // Create all transports
      const httpInboundTransport = new DidCommHttpInboundTransport({ port: createAgentDto.port })
      const httpOutboundTransport = new DidCommHttpOutboundTransport()
      const wsInboundTransport = new DidCommWsInboundTransport({ server: this.socketServer })
      const wsOutboundTransport = new DidCommWsOutboundTransport()
      console.log("Created transports");

      // Register all Transports
      this.agent.didcomm.registerInboundTransport(httpInboundTransport)
      this.agent.didcomm.registerOutboundTransport(httpOutboundTransport)
      this.agent.didcomm.registerInboundTransport(wsInboundTransport)
      this.agent.didcomm.registerOutboundTransport(wsOutboundTransport)
      console.log("Registered transports");

      // Initialize agent
      console.log("Agent pre-initialize");
      await this.agent.initialize().catch(console.error);
      console.log("Agent initialized");

      httpInboundTransport.server?.on('upgrade', (request, socket, head) => {
        this.socketServer.handleUpgrade(request, socket as Socket, head, (socket) => {
          this.socketServer.emit('connection', socket, request)
        })
      })

      return "OK";
 }


  async createInvitation(): Promise<String> {
    var { outOfBandRecord, invitation } = await this.agent.didcomm.oob.createLegacyInvitation({ label: this.label })
    var invite = {
      invitationUrl: invitation.toUrl({ domain: this.agent.didcomm.config.endpoints[0] }),
      outOfBandRecord
    }
    return invite.invitationUrl
  }

  async createOOBInvitation(): Promise<String> {
    const outOfBandRecord = await this.agent.didcomm.oob.createInvitation({ multiUseInvitation: true, label: this.label })
    var invite = {
      invitationUrl: outOfBandRecord.outOfBandInvitation.toUrl({ domain: this.agent.didcomm.config.endpoints[0] }),
      outOfBandRecord,
    }
    return invite.invitationUrl
  }


  setupProofRequestListener() {
    console.log("Listen for proof")
    this.agent.events.on(DidCommProofEventTypes.ProofStateChanged, async ({ payload }: DidCommProofStateChangedEvent) => {
      //console.log("Proof presentation=",payload.proofRecord )
      console.log("Proof state: ", payload.proofRecord?.state)
      console.log("Proof verified: ", payload.proofRecord?.isVerified ? 'Verified' : 'not Verified')
    })
  }

  setupMessageListener() {
    console.log("Listen for messages")
    this.agent.events.on(DidCommBasicMessageEventTypes.DidCommBasicMessageStateChanged, async ({ payload }: DidCommBasicMessageStateChangedEvent) => {
      if (payload.basicMessageRecord.role === DidCommBasicMessageRole.Receiver) {
        console.log("Message:",payload.message.content);
      }
    })
  }

  setupConnectionListener = () => {
    this.agent.events.on<DidCommConnectionStateChangedEvent>(DidCommConnectionEventTypes.DidCommConnectionStateChanged, ({ payload }) => {
      if (payload.connectionRecord.state === DidCommDidExchangeState.Completed) {
        // the connection is now ready for usage in other protocols!
        console.log("Connection completed", payload.connectionRecord)
        //this.afjAgent.connection_id = payload.connectionRecord.id
        //process.exit(0)
      }
      else {
        console.log("Connection status", payload.connectionRecord)
      }
    })
  }

}
