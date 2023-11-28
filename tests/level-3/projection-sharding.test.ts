import assert from 'assert';
import sinon from 'sinon';
import { randomBytes } from 'crypto';
import { Broker, EventType, Projection, Stream, StreamConfig } from '../../src/level-3/projection-sharding';

describe('Projection Sharding', () => {
  describe('Projection Stream', () => {
    const callback = sinon.stub();

    afterEach(function () {
      callback.reset();
    });

    test('should send events in sequence', async () => {
      const stream = new Stream();
      const promise = new Promise<void>((resolve) => {
        stream.on('main', (event) => {
          if (event.id === 'EOF') return resolve();
          callback.onCall(event.aggregate.version).returns(event.type);
        });
      });

      stream.sendEvents([
        {
          stream: 'main',
          events: [
            {
              id: randomBytes(13),
              type: EventType.WalletCredited,
              aggregate: {
                id: randomBytes(13),
                version: 0
              },
              body: { amount: 100 },
              timestamp: Date.now(),
            },
            {
              id: randomBytes(13),
              type: EventType.WalletDebited,
              aggregate: {
                id: randomBytes(13),
                version: 1
              },
              body: { amount: 100 },
              timestamp: Date.now(),
            },
            /* stopper event */
            { id: 'EOF' }
          ]
        }
      ]);

      await promise;

      assert.equal(callback(), EventType.WalletCredited);
      assert.equal(callback(), EventType.WalletDebited);
      assert.equal(callback(), undefined);
    });

    test('should accept events in sequence', async () => {
      const stream = new Stream();
      const promise = new Promise<void>((resolve) => {
        stream.subscribe('main', (event) => {
          if (event.id === 'EOF') return resolve();
          callback.onCall(event.aggregate.version).returns(event.type);
        });
      });

      [
        {
          id: randomBytes(13),
          type: EventType.WalletCredited,
          aggregate: {
            id: randomBytes(13),
            version: 0
          },
          body: { amount: 100 },
          timestamp: Date.now(),
        },
        {
          id: randomBytes(13),
          type: EventType.WalletDebited,
          aggregate: {
            id: randomBytes(13),
            version: 1
          },
          body: { amount: 100 },
          timestamp: Date.now(),
        },
        /* stopper event */
        { id: 'EOF' }
      ].forEach((event) => { stream.emit('main', event); });

      await promise;

      assert.equal(callback(), EventType.WalletCredited);
      assert.equal(callback(), EventType.WalletDebited);
      assert.equal(callback(), undefined);
    })
  });

  describe('Projection Stream Configuration', () => {
    test('should save stream information', () => {
      const config = new StreamConfig();

      config.save({ id: 'wallet', events: [1, 2] });
      config.save({ id: 'account', events: [3, 4] });

      assert.equal(config.streams.length, 2);
      assert.deepEqual(
        config.streams,
        [{ id: 'wallet', events: [1, 2] }, { id: 'account', events: [3, 4] }],
      );
    });

    test('should find stream information', () => {
      const config = new StreamConfig();

      config.streams.push({ id: 'wallet', events: [1, 2] });
      config.streams.push({ id: 'account', events: [3, 4] });

      const streams = config.findStreams(2);

      assert.deepEqual(streams, [{ id: 'wallet', events: [1, 2] }]);
    });
  });

  describe('Projection', () => {
    test('should accept events in the main stream', () => {
      const callback = sinon.fake();

      const stream = new Stream();
      const config = new StreamConfig();
      const broker = new Broker(config, stream);

      const projection = new Projection(
        'wallet',
        stream,
        config,
        [
          {
            type: EventType.WalletCredited,
            handle: (event) => {
              callback(event);
            },
          },
          {
            type: EventType.WalletDebited,
            handle: (event) => {
              callback(event);
            },
          },
        ],
      );

      projection.start();
      broker.start();

      stream.sendEvents([
        {
          stream: 'main',
          events: [
            {
              id: randomBytes(13),
              type: EventType.WalletCredited,
              aggregate: {
                id: randomBytes(13),
                version: 1
              },
              body: { amount: 100 },
              timestamp: Date.now(),
            },
            {
              id: randomBytes(13),
              type: EventType.WalletDebited,
              aggregate: {
                id: randomBytes(13),
                version: 2
              },
              body: { amount: 100 },
              timestamp: Date.now(),
            }
          ]
        }
      ]);

      assert(callback.calledTwice);
    });

    test('should work with multi projection setup', () => {
      const callback = sinon.fake();

      const stream = new Stream();
      const config = new StreamConfig();
      const broker = new Broker(config, stream);
      const handle = (event: any) => callback(event);

      const wallet = new Projection(
        'wallet',
        stream,
        config,
        [
          {
            type: EventType.WalletCredited,
            handle
          },
          {
            type: EventType.WalletDebited,
            handle
          },
        ],
      );

      const account = new Projection(
        'account',
        stream,
        config,
        [
          {
            type: EventType.AccountCreated,
            handle
          },
          {
            type: EventType.AccountUpdated,
            handle
          },
          {
            type: EventType.AccountDeleted,
            handle
          },
        ],
      );

      wallet.start();
      account.start();
      broker.start();

      stream.sendEvents([
        {
          stream: 'main',
          events: [
            {
              id: randomBytes(13),
              type: EventType.AccountCreated,
              aggregate: {
                id: randomBytes(13),
                version: 1
              },
              body: { amount: 100 },
              timestamp: Date.now(),
            },
            {
              id: randomBytes(13),
              type: EventType.WalletCredited,
              aggregate: {
                id: randomBytes(13),
                version: 2
              },
              body: { amount: 100 },
              timestamp: Date.now(),
            },
            {
              id: randomBytes(13),
              type: EventType.AccountUpdated,
              aggregate: {
                id: randomBytes(13),
                version: 3
              },
              body: { amount: 100 },
              timestamp: Date.now(),
            },
            {
              id: randomBytes(13),
              type: EventType.WalletDebited,
              aggregate: {
                id: randomBytes(13),
                version: 4
              },
              body: { amount: 100 },
              timestamp: Date.now(),
            },
            {
              id: randomBytes(13),
              type: EventType.AccountDeleted,
              aggregate: {
                id: randomBytes(13),
                version: 5
              },
              body: { amount: 100 },
              timestamp: Date.now(),
            }
          ]
        }
      ]);

      sinon.assert.callCount(callback, 5);
    });
  });
});
