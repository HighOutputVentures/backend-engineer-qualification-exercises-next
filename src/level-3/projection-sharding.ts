import { EventEmitter } from 'node:events';

export enum EventType {
  WalletCredited = 1,
  WalletDebited = 2,

  AccountCreated = 3,
  AccountUpdated = 4,
  AccountDeleted = 5,
};

export class Stream extends EventEmitter {
  sendEvents(params: { stream: string, events: any[] }[]) {
    /* put implementation here */
  }

  subscribe(stream: string, handle: (event: any) => void) {
    /* put implementation here */
  }
}

export  class StreamConfig {
  private readonly _streams: { id: string, events: number[] }[] = [];

  get streams() {
    return this._streams;
  }

  save(params: { id: string, events: number[] }) {
    /* put implementation here */
  }

  findStreams(eventType: number): string[] {
    /* put implementation here */
  }
}

export class Broker {
  constructor(
    private readonly config: StreamConfig,
    private readonly stream: Stream
  ) {}

  start() {
    /* put implementation here */
  }
}

export class Projection{
  private readonly eventHandlers: Map<
    number,
    { type: number, handle: (event: any) => any }
  >;

  constructor(
    private id: string,
    private readonly stream: Stream,
    private readonly config: StreamConfig,
    eventHandlers: { type: number, handle: (event: any) => any }[],
  ) {}

  private async handle(event: any) {
    /* put implementation here */
  }

  start () {
    /* put implementation here */
  }
}
