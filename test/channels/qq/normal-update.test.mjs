import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { WebSocketServer } from 'ws';
import { QQBot } from '@tencent-connect/qqbot-nodejs';
import { QqRuntime } from '../../../src/channels/qq/qq-runtime.mjs';
import { QqStateStore } from '../../../src/channels/qq/state-store.mjs';
import { QqController } from '../../../src/channels/qq/qq-controller.mjs';
import { QqConfigStore, deriveQqBotIdentity } from '../../../src/channels/qq/config-store.mjs';
import { Context, Service } from '@deepseek-ai/cordis';
import { createDeliveryService } from '../../../plugin-src/host/delivery-service.mjs';
import { createDeliveryAdapter } from '../../../plugin-src/host/delivery-adapter.mjs';
import { installProductionChannel } from '../../../plugin-src/host/channels/shared/startup.mjs';
import { managementFetch } from '../../fixtures/management-rpc.mjs';

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'qq-normal-update-'));
  const path = join(directory, 'state.json');
  const handshakes = [];
  const offline = [];
  const posts = [];
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    const data = request.url.includes('getAppAccessToken') ? { access_token: 'fixture-token', expires_in: 7200 }
      : request.url === '/gateway' ? { url: `ws://127.0.0.1:${server.address().port}` }
        : request.url === '/users/@me' ? { id: 'native-bot', bot: true }
          : (posts.push(JSON.parse(body)), { id: `sent-${posts.length}` });
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify(data));
  });
  const sockets = new WebSocketServer({ server });
  let socket;
  sockets.on('connection', client => {
    socket = client;
    client.send(JSON.stringify({ op: 10, d: { heartbeat_interval: 60000 } }));
    client.on('message', bytes => {
      const request = JSON.parse(bytes.toString());
      if (![2, 6].includes(request.op)) return;
      handshakes.push(request);
      client.send(JSON.stringify({ op: 0, t: request.op === 6 ? 'RESUMED' : 'READY',
        s: request.op === 6 ? request.d.seq + (offline.length ? 0 : 1) : 1,
        d: { session_id: 'gateway-session', user: { id: 'native-bot', bot: true } } }));
      if (request.op === 6) for (const payload of offline.splice(0)) client.send(JSON.stringify(payload));
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const url = `http://127.0.0.1:${server.address().port}`;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (input, options) => {
    assert.equal(new URL(String(input)).origin, url, 'HTTP fixture must stay on loopback');
    return originalFetch(input, options);
  };
  const runtimes = [];
  t.after(async () => {
    for (const runtime of runtimes) await runtime.stop({ drain: false });
    for (const client of sockets.clients) client.terminate();
    await new Promise(resolve => sockets.close(resolve));
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    globalThis.fetch = originalFetch;
    await rm(directory, { recursive: true, force: true });
  });
  const runtime = (state, accept, beforeListener, config = { botId: 'qq-fixture', appId: 'application-one', consumerMode: 'external-consumer' }) => {
    const value = new QqRuntime({ config,
      appSecret: 'fixture-secret', state, harness: {}, externalConsumer: accept,
      logger: { debug() {}, info() {}, warn() {}, error() {} }, connectTimeoutMs: 5000,
      createBot: options => {
        const bot = new QQBot({ ...options, baseUrl: url, tokenBaseUrl: url });
        if (beforeListener) bot.on('message', beforeListener);
        return bot;
      } });
    runtimes.push(value);
    return value;
  };
  const send = (sequence = 2) => socket.send(JSON.stringify({ op: 0, s: sequence, t: 'GROUP_AT_MESSAGE_CREATE',
    d: { id: 'original-message', group_openid: 'original-group', author: { member_openid: 'original-member' },
      content: 'question', timestamp: new Date().toISOString() } }));
  const queueOffline = () => offline.push({ op: 0, s: 3, t: 'GROUP_AT_MESSAGE_CREATE',
    d: { id: 'offline-message', group_openid: 'original-group', author: { member_openid: 'original-member' },
      content: 'received while stopped', timestamp: new Date().toISOString() } });
  return { path, runtime, send, handshakes, posts, queueOffline };
}

test('normal shutdown waits for callbacks and canonical acceptance before saving a resumable cursor', async t => {
  const fx = await fixture(t);
  const priorStarted = deferred();
  const priorContinue = deferred();
  const acceptedStarted = deferred();
  const acceptedContinue = deferred();
  let source;
  const state = await new QqStateStore(fx.path).load();
  const runtime = fx.runtime(state, async event => {
    source = event;
    acceptedStarted.resolve();
    await acceptedContinue.promise;
    return { accepted: true };
  }, async () => { priorStarted.resolve(); await priorContinue.promise; });
  await runtime.start();
  fx.send();
  await priorStarted.promise;
  let stopped = false;
  const stopping = runtime.stop().then(() => { stopped = true; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(stopped, false);
  priorContinue.resolve();
  await acceptedStarted.promise;
  assert.equal(stopped, false);
  assert.equal(state.snapshot().gatewaySession, undefined);
  assert.equal((await runtime.describeDeliveryAccount()).appId, 'application-one');
  acceptedContinue.resolve();
  await stopping;
  assert.equal(state.snapshot().gatewaySession.lastSeq, 2);
  fx.queueOffline();
  const restoredState = await new QqStateStore(fx.path).load();
  const replayed = deferred();
  const replayedMessages = [];
  const restored = fx.runtime(restoredState, async event => {
    replayedMessages.push(event.messageId); replayed.resolve(); return { accepted: true };
  });
  await restored.start();
  await replayed.promise;
  assert.deepEqual(replayedMessages, ['offline-message']);
  assert.equal(fx.handshakes[1].op, 6);
  assert.equal(fx.handshakes[1].d.session_id, 'gateway-session');
  assert.equal(fx.handshakes[1].d.seq, 2);
  const receipt = await restored.replyChecked(source.reply, 'later answer',
    { signal: new AbortController().signal, beforeSend: () => true, receipt: true });
  assert.equal(receipt.receipt.messageId, 'sent-1');
  assert.equal(fx.posts[0].msg_id, 'original-message');
});

test('canonical rejection swallowed by the SDK still prevents a shutdown checkpoint', async t => {
  const fx = await fixture(t);
  const handled = deferred();
  const state = await new QqStateStore(fx.path).load();
  const runtime = fx.runtime(state, async () => { handled.resolve(); return { accepted: false }; });
  await runtime.start();
  fx.send();
  await handled.promise;
  await assert.rejects(() => runtime.stop(), { code: 'admission-unresolved' });
  assert.equal((await new QqStateStore(fx.path).load()).snapshot().gatewaySession, undefined);
  assert.equal(fx.posts.length, 0);
});

test('controller close retains the receiver lease and account read until durable admission completes', async t => {
  const fx = await fixture(t);
  const state = await new QqStateStore(fx.path).load();
  const config = await new QqConfigStore(`${fx.path}.config`).load();
  const identity = deriveQqBotIdentity('application-one');
  await config.save({ ...identity, appId: 'application-one', ownerUserOpenid: 'owner', consumerMode: 'external-consumer' });
  const controller = new QqController({ configStore: config, qrAuth: { start() {} },
    credentials: { resolve: async () => ({ value: 'fixture-secret' }), set: async () => {}, unset: async () => {} },
    createRuntime: async args => fx.runtime(state, args.externalConsumer, undefined, args.config),
    logger: { debug() {}, info() {}, warn() {}, error() {} },
  });
  t.after(() => controller.close({ drain: false }));
  await controller.initialize();
  const account = await controller.describeDeliveryAccount(identity.botId);
  const admitted = deferred();
  const continueAdmission = deferred();
  let acceptanceSignal;
  await controller.consumeInbound(identity.botId, { expectedFingerprint: account.account.fingerprint,
    onEvent: async (_event, { signal }) => {
      acceptanceSignal = signal;
      admitted.resolve();
      await continueAdmission.promise;
      assert.equal(signal.aborted, false);
      assert.equal((await controller.describeDeliveryAccount(identity.botId)).account.fingerprint, account.account.fingerprint);
      return { accepted: true };
    } });
  fx.send();
  await admitted.promise;
  const closing = controller.close();
  await assert.rejects(() => controller.startProvisioning(), /closed/);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(acceptanceSignal.aborted, false);
  continueAdmission.resolve();
  await closing;
  assert.equal(acceptanceSignal.aborted, true);
  assert.equal((await new QqStateStore(fx.path).load()).snapshot().gatewaySession.lastSeq, 2);
});

test('native dependent disposal drains the original provider before releasing its receiver lease', async t => {
  const fx = await fixture(t);
  const state = await new QqStateStore(fx.path).load();
  const config = await new QqConfigStore(`${fx.path}.config`).load();
  const identity = deriveQqBotIdentity('application-one');
  await config.save({ ...identity, appId: 'application-one', ownerUserOpenid: 'owner', consumerMode: 'external-consumer' });
  const controller = new QqController({ configStore: config, qrAuth: { start() {} },
    credentials: { resolve: async () => ({ value: 'fixture-secret' }), set: async () => {}, unset: async () => {} },
    createRuntime: async args => fx.runtime(state, args.externalConsumer, undefined, args.config),
    logger: { debug() {}, info() {}, warn() {}, error() {} },
  });
  const delivery = createDeliveryService();
  const adapter = createDeliveryAdapter({ channel: 'qq', coreController: controller, stateFor: async () => state,
    workspaces: { has: id => id === identity.botId, listBotIds: () => [identity.botId], listDeliveryTargets: () => [] } });
  const ctx = new Context();
  class Connection extends Service {
    constructor(context) { super(context, 'connection'); }
    get fetch() { return managementFetch(() => this.ctx.effect(() => () => {}, 'fixture management route')); }
  }
  new Connection(ctx);
  const publicService = {
    consumeInbound: (id, options) => delivery.consumeInbound(id, options),
    quiesceInbound: (id, options) => delivery.quiesceInbound(id, options),
    describeBot: id => delivery.describeBot(id),
  };
  const provider = ctx.plugin({ name: 'qq-provider-update-fixture', inject: ['connection'], async apply(child) {
    child.provide('dshIm', publicService);
    await controller.initialize();
    await installProductionChannel(child, { deliveryService: delivery }, { channel: 'qq', rpcChannel: '/qq',
      createProduction: async () => ({ controller, deliveryAdapter: adapter,
        prepareClose: () => controller.close(), close: () => controller.close() }),
      createHandler: () => async () => ({ ok: true }),
    });
  } });
  await provider;
  const account = await delivery.describeBot(identity.botId);
  const admitted = deferred();
  const commit = deferred();
  const receiver = new AbortController();
  let eventSignal;
  let drainReceipt;
  const consumer = ctx.plugin({ name: 'qq-consumer-update-fixture', inject: ['dshIm'], async apply(child) {
    const service = child.get('dshIm');
    const dispose = await service.consumeInbound(identity.botId, {
      expectedFingerprint: account.account.fingerprint, signal: receiver.signal,
      onEvent: async (_event, { signal }) => {
        eventSignal = signal;
        admitted.resolve();
        await commit.promise;
        assert.equal(signal.aborted, false);
        assert.equal((await service.describeBot(identity.botId)).account.fingerprint, account.account.fingerprint);
        return { accepted: true };
      },
    });
    child.effect(() => async () => {
      drainReceipt = await service.quiesceInbound(identity.botId, { expectedFingerprint: account.account.fingerprint });
      receiver.abort();
      dispose();
    }, 'drain before removing the consumer');
  } });
  await consumer;
  t.after(async () => { commit.resolve(); await consumer.dispose(); await provider.dispose(); });
  fx.send();
  await admitted.promise;
  const closing = provider.dispose();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(eventSignal.aborted, false);
  assert.equal(state.snapshot().gatewaySession, undefined);
  commit.resolve();
  await closing;
  assert.deepEqual(drainReceipt, { stopped: true, resumeCursor: true });
  assert.equal(eventSignal.aborted, true);
  assert.equal((await new QqStateStore(fx.path).load()).snapshot().gatewaySession.lastSeq, 2);
});

test('a quiesced receiver stays stopped until a new exclusive consumer registers', async t => {
  const fx = await fixture(t);
  const state = await new QqStateStore(fx.path).load();
  const config = await new QqConfigStore(`${fx.path}.config`).load();
  const identity = deriveQqBotIdentity('application-one');
  await config.save({ ...identity, appId: 'application-one', ownerUserOpenid: 'owner', consumerMode: 'external-consumer' });
  const controller = new QqController({ configStore: config, qrAuth: { start() {} },
    credentials: { resolve: async () => ({ value: 'fixture-secret' }), set: async () => {}, unset: async () => {} },
    createRuntime: async args => fx.runtime(state, args.externalConsumer, undefined, args.config),
    logger: { debug() {}, info() {}, warn() {}, error() {} },
  });
  t.after(() => controller.close({ drain: false }));
  await controller.initialize();
  const fingerprint = (await controller.describeDeliveryAccount(identity.botId)).account.fingerprint;
  const admitted = deferred();
  let source;
  const dispose = await controller.consumeInbound(identity.botId, { expectedFingerprint: fingerprint,
    onEvent: async event => { source = event; admitted.resolve(); return { accepted: true }; } });
  fx.send();
  await admitted.promise;
  assert.deepEqual(await controller.quiesceInbound(identity.botId, { expectedFingerprint: fingerprint }),
    { stopped: true, resumeCursor: true });
  const before = fx.handshakes.length;
  await controller.initialize();
  await assert.rejects(() => controller.reconnectBot(identity.botId), { code: 'provider-unavailable' });
  await assert.rejects(() => controller.consumeInbound(identity.botId,
    { expectedFingerprint: fingerprint, onEvent: async () => ({ accepted: true }) }), { code: 'consumer-conflict' });
  assert.equal(fx.handshakes.length, before);
  dispose();
  assert.deepEqual(await controller.resumeInbound(identity.botId, { expectedFingerprint: fingerprint }), { connected: false, accountVerified: true });
  assert.equal((await controller.describeDeliveryAccount(identity.botId)).connected, false);
  await controller.consumeInbound(identity.botId,
    { expectedFingerprint: fingerprint, onEvent: async () => ({ accepted: true }) });
  assert.deepEqual(await controller.qualifyReplyChecked(identity.botId, source.reply,
    { expectedFingerprint: fingerprint }), source.reply);
  assert.ok(fx.handshakes.slice(before).every(value => value.op === 6 && value.d.seq === 2));
  const service = createDeliveryService();
  const unregister = service.registerAdapter(createDeliveryAdapter({ channel: 'qq', coreController: controller,
    stateFor: async () => state, workspaces: { has: id => id === identity.botId } }));
  t.after(unregister);
  assert.deepEqual(await service.releaseSource(identity.botId, source.reply,
    { expectedFingerprint: fingerprint }), { released: true });
  assert.equal(state.replyProof(fingerprint, source.messageId), undefined);
});
