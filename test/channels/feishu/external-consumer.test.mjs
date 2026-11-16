import assert from 'node:assert/strict';
import test from 'node:test';
import { createServer } from 'node:http';
import { normalizeExternalText, ExclusiveInboundConsumers } from '../../../src/channels/feishu/external-consumer.mjs';

const fingerprint = 'a'.repeat(64);
function event() {
  return {
    event_id: 'event-1', app_id: 'app',
    sender: { sender_type: 'user', sender_id: { open_id: 'actor' } },
    message: { message_id: 'message', chat_id: 'conversation', chat_type: 'group',
      message_type: 'text', content: JSON.stringify({ text: '@_user_1 hello' }),
      mentions: [{ id: { open_id: 'bot-open-id' }, key: '@_user_1', name: 'QA Bot' }],
      create_time: '1790787600000', thread_id: 'thread', root_id: 'root', parent_id: 'parent' },
  };
}
function normalized(value = event()) {
  return normalizeExternalText(value, { botId: 'bot', appId: 'app', botOpenId: 'bot-open-id', fingerprint });
}

test('normalized authenticated text retains causal identities without raw payload or secrets', () => {
  const input = event();
  input.secret = 'private-material';
  const evidence = normalized(input);
  assert.deepEqual(evidence.reply, { messageId: 'message', conversationId: 'conversation', actorId: 'actor',
    threadId: 'thread', rootId: 'root', parentId: 'parent' });
  assert.equal(evidence.replay.gapPossible, true);
  assert.equal(Object.isFrozen(evidence.reply), true);
  assert.equal(JSON.stringify(evidence).includes('private-material'), false);
  input.message.chat_id = 'other';
  assert.equal(evidence.conversation.id, 'conversation');
  assert.throws(() => normalized({ ...input, app_id: 'other' }), { code: 'account-changed' });
  assert.equal(normalized({ ...input, sender: { sender_type: 'bot' } }), null);
  assert.equal(evidence.conversation.kind, 'group');
  assert.equal(evidence.mentionedAccount, true);
  assert.deepEqual(evidence.mentions, [{ id: 'bot-open-id', key: '@_user_1', name: 'QA Bot' }]);
  assert.equal(Object.isFrozen(evidence.mentions[0]), true);
  assert.equal(normalized({ ...input, message: { ...input.message, chat_type: 'unknown' } }), null);
});

test('an external Bot is admitted only when it mentions this account in a group', () => {
  const bot = { sender_type: 'app', sender_id: { open_id: 'peer-bot' }, sender_name: 'Peer Bot' };
  const evidence = normalized({ ...event(), sender: bot });
  assert.deepEqual(evidence.actor, { kind: 'bot', id: 'peer-bot', name: 'Peer Bot' });
  assert.equal(evidence.mentionedAccount, true);
  assert.equal(evidence.reply.actorId, 'peer-bot');
  assert.equal(normalized({ ...event(), sender: { ...bot, sender_type: 'bot' } }).actor.kind, 'bot');
  const unaddressed = event();
  unaddressed.message.mentions = [{ id: { open_id: 'someone-else' }, key: '@_user_1', name: 'Other' }];
  assert.equal(normalized({ ...unaddressed, sender: bot }), null);
  assert.equal(normalized({ ...event(), sender: { ...bot, sender_id: { open_id: 'bot-open-id' } } }), null);
  assert.equal(normalized({ ...event(), sender: { ...bot, sender_id: { open_id: 'x', app_id: 'app' } } }), null);
  const direct = event();
  direct.message.chat_type = 'p2p';
  assert.equal(normalized({ ...direct, sender: bot }), null);
  assert.equal(normalized(event()).actor.kind, 'user');
});

test('exclusive registration waits for durable acceptance and disposal never substitutes another consumer', async () => {
  const consumers = new ExclusiveInboundConsumers();
  let commit;
  const committed = new Promise(resolve => { commit = resolve; });
  let calls = 0;
  const dispose = consumers.register('bot', { fingerprint, onEvent: async () => {
    calls++; await committed; return { accepted: true };
  } });
  assert.throws(() => consumers.register('bot', { fingerprint, onEvent() {} }), { code: 'consumer-conflict' });
  let acknowledged = false;
  const pending = consumers.accept('bot', normalized()).then(() => { acknowledged = true; });
  await Promise.resolve();
  assert.equal(acknowledged, false);
  commit(); await pending;
  assert.equal(calls, 1);
  dispose(); dispose();
  await assert.rejects(consumers.accept('bot', normalized()), { code: 'consumer-unavailable' });
});

test('consumer loss during commit rejects acknowledgement even when a new registration appears', async () => {
  const consumers = new ExclusiveInboundConsumers();
  let commit;
  const committed = new Promise(resolve => { commit = resolve; });
  const dispose = consumers.register('bot', { fingerprint, onEvent: async () => {
    await committed; return { accepted: true };
  } });
  const pending = consumers.accept('bot', normalized());
  dispose();
  consumers.register('bot', { fingerprint, onEvent: async () => ({ accepted: true }) });
  commit();
  await assert.rejects(pending, { code: 'consumer-unavailable' });
  consumers.close();
});

test('fingerprint mismatch, uncommitted result and canceled registrations fail closed', async () => {
  const consumers = new ExclusiveInboundConsumers();
  const controller = new AbortController();
  let calls = 0;
  consumers.register('bot', { fingerprint, signal: controller.signal, onEvent: async () => {
    calls++; return { accepted: false };
  } });
  await assert.rejects(consumers.accept('bot', { ...normalized(), fingerprint: 'b'.repeat(64) }), { code: 'account-changed' });
  assert.equal(calls, 0);
  await assert.rejects(consumers.accept('bot', normalized()), { code: 'ingress-not-accepted' });
  controller.abort();
  await assert.rejects(consumers.accept('bot', normalized()), { code: 'consumer-unavailable' });
});


test('ordinary group text and another Bot mention retain context without implying attention for this account', () => {
  const input = event();
  input.message.mentions = [{ id: { open_id: 'other-bot' }, key: '@_user_2' }];
  const otherMention = normalized(input);
  assert.equal(otherMention.mentionedAccount, false);
  assert.deepEqual(otherMention.mentions, [{ id: 'other-bot', key: '@_user_2' }]);
  input.message.mentions = [];
  input.message.content = JSON.stringify({ text: 'ordinary team context' });
  assert.equal(normalized(input).mentionedAccount, false);
  assert.equal(normalized(input).text, 'ordinary team context');
  input.message.chat_type = 'p2p';
  assert.equal(normalized(input).conversation.kind, 'dm');
});

test('group evidence requires authenticated account identity and valid mention and timestamp fields', () => {
  assert.throws(() => normalizeExternalText(event(), { botId: 'bot', appId: 'app', fingerprint }), { code: 'account-unverified' });
  const input = event();
  input.message.mentions = [{ id: { open_id: 'bot-open-id' }, key: '' }];
  assert.throws(() => normalized(input), { code: 'invalid-inbound' });
  input.message.mentions = [];
  input.message.create_time = '9e99';
  assert.throws(() => normalized(input), { code: 'invalid-inbound' });
});

test('preserves the platform tenant and complete typed mentions without treating message text as identity', () => {
  const input = event();
  input.tenant_key = 'tenant-current';
  input.sender.tenant_key = 'tenant-current';
  input.message.mentions = [
    { key: '@_user_1', id: { open_id: 'bot-open-id' }, tenant_key: 'tenant-current', mentioned_type: 'app' },
    { key: '@_user_2', id: { open_id: 'other-bot' }, tenant_key: 'tenant-current', mentioned_type: 'bot' },
    { key: '@_user_3', id: { open_id: 'human' }, tenant_key: 'tenant-current', mentioned_type: 'user' },
  ];
  const result = normalized(input);
  assert.equal(result.appId, 'app');
  assert.equal(result.tenantId, 'tenant-current');
  assert.deepEqual(result.mentions.map(value => [value.id, value.tenantId, value.kind]), [
    ['bot-open-id', 'tenant-current', 'bot'], ['other-bot', 'tenant-current', 'bot'], ['human', 'tenant-current', 'user'],
  ]);
  assert.throws(() => normalized({ ...input, sender: { ...input.sender, tenant_key: 'other' } }), { code: 'invalid-inbound' });
  assert.throws(() => normalized({ ...input, header: { tenant_key: 'other' } }), { code: 'invalid-inbound' });
  assert.throws(() => normalized({ ...input, message: { ...input.message,
    mentions: [{ key: '@_user_1', id: { open_id: 'other-bot' }, id_type: 'user_id' }] } }), { code: 'invalid-inbound' });
  const future = normalized({ ...input, message: { ...input.message,
    mentions: [{ key: '@_user_1', id: { open_id: 'other-bot' }, mentioned_type: 'future-type' }] } });
  assert.equal(future.mentions[0].mentionedType, 'future-type');
  assert.equal(future.mentions[0].kind, undefined);
  assert.throws(() => normalized({ ...input, message: { ...input.message,
    mentions: [{ key: '@_user_1', id: { open_id: 'other-bot' }, mentioned_type: 1 }] } }), { code: 'invalid-inbound' });
  const ordinary = event();
  ordinary.sender.tenant_key = 'sender-only';
  ordinary.message.content = JSON.stringify({ text: '@other-bot tenant-current' });
  ordinary.message.mentions = [];
  assert.equal(normalized(ordinary).tenantId, undefined);
  assert.deepEqual(normalized(ordinary).mentions, []);
});


test('file metadata is explicitly negotiated while legacy text consumers retain their version 1 envelope', () => {
  const consumers = new ExclusiveInboundConsumers();
  const legacy = consumers.register('legacy', { fingerprint, onEvent: async () => ({ accepted: true }) });
  assert.equal(consumers.acceptsFiles('legacy'), false);
  consumers.register('files', { fingerprint, sourceFiles: true, onEvent: async () => ({ accepted: true }) });
  assert.equal(consumers.acceptsFiles('files'), true);
  legacy();
  assert.equal(consumers.acceptsFiles('legacy'), false);
  consumers.close();
});

test('own app echo is opt-in, authenticated separately, and never admitted as Human text', async () => {
  const { normalizeOwnTextEcho } = await import('../../../src/channels/feishu/external-consumer.mjs');
  const identity = { botId: 'own', appId: 'cli_own', botOpenId: 'ou_bot', fingerprint: 'a'.repeat(64) };
  const raw = { app_id: 'cli_own', event_id: 'event', sender: { sender_type: 'app', sender_id: { app_id: 'cli_own' } }, message: { message_type: 'text', chat_type: 'group', message_id: 'om_own', chat_id: 'oc_group', content: '{"text":"report"}', create_time: '1791012000000' } };
  const echo = normalizeOwnTextEcho(raw, identity);
  assert.equal(echo.messageId, 'om_own');
  assert.equal(normalizeExternalText(raw, identity), null);
  assert.equal(normalizeOwnTextEcho({ ...raw, sender: { sender_type: 'app', sender_id: { app_id: 'cli_other' } } }, identity), null);
  const consumers = new ExclusiveInboundConsumers();
  let events = 0, echoes = 0;
  const dispose = consumers.register('own', { fingerprint: identity.fingerprint, onEvent: async () => { events++; return { accepted: true }; }, onEcho: async value => { echoes++; assert.equal(value.messageId, 'om_own'); return { accepted: true }; } });
  await consumers.accept('own', echo, undefined, true);
  assert.equal(events, 0); assert.equal(echoes, 1); dispose();
  await assert.rejects(consumers.accept('own', echo, undefined, true), { code: 'consumer-unavailable' });
});

test('typed source inspection works inside first consumer acceptance through the official SDK', { timeout: 5000 }, async () => {
  const lark = await import('@larksuiteoapi/node-sdk');
  const { verifyFeishuApp } = await import('../../../src/channels/feishu/feishu-app.mjs');
  const { FeishuRuntime } = await import('../../../src/channels/feishu/feishu-runtime.mjs');
  const { MultiBotDshFeishuController } = await import('../../../src/channels/feishu/multi-bot-controller.mjs');
  const { createDeliveryAdapter } = await import('../../../plugin-src/host/delivery-adapter.mjs');
  const { createDeliveryService } = await import('../../../plugin-src/host/delivery-service.mjs');
  const configs = ['a', 'b'].map(suffix => ({ id: `account_${suffix}`, appId: `cli_${suffix}`,
    botOpenId: `ou_bot_${suffix}`, domain: 'feishu', secretRef: suffix, ownerOpenIds: ['*'], consumerMode: 'external-consumer' }));
  const requests = [];
  let changed = false;
  const mentions = configs.map((config, index) => ({ key: `@_user_${index + 1}`, id: { open_id: config.botOpenId },
    tenant_key: 'tenant-current', mentioned_type: 'bot', name: 'Research Bot' }));
  mentions.push({ key: '@_user_3', id: { open_id: 'ou-human' }, tenant_key: 'tenant-current', mentioned_type: 'user', name: 'Researcher' });
  const original = { message_id: 'om-original', chat_id: 'oc-original', chat_type: 'group', message_type: 'text',
    content: JSON.stringify({ text: '@_user_1 @_user_2 review this result' }), mentions, create_time: '1790787600000' };
  const apiMessage = { message_id: original.message_id, chat_id: original.chat_id, msg_type: 'text', updated: false,
    create_time: original.create_time, body: { content: original.content },
    sender: { sender_type: 'user', id_type: 'open_id', id: 'ou-requester', sender_name: 'Researcher' },
    mentions: mentions.map(({ mentioned_type: _type, ...mention }, index) => ({ ...mention,
      id: configs[index]?.appId ?? mention.id.open_id, id_type: configs[index] ? 'app_id' : 'open_id' })) };
  const server = createServer(async (request, response) => {
    const chunks = []; for await (const chunk of request) chunks.push(chunk);
    const body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : undefined;
    const path = new URL(request.url, 'http://localhost').pathname;
    requests.push({ path, method: request.method });
    let result;
    if (path === '/open-apis/auth/v3/tenant_access_token/internal') {
      assert.equal(body.app_secret, 'local-fixture-secret');
      assert.ok(configs.some(config => config.appId === body.app_id));
      result = { code: 0, tenant_access_token: `local-token-${body.app_id}`, expire: 7200 };
    } else {
      const config = configs.find(row => request.headers.authorization === `Bearer local-token-${row.appId}`);
      assert.ok(config, 'The SDK and verifier must authenticate their official reads');
      if (path === '/open-apis/bot/v3/info/') result = { code: 0,
        bot: { app_name: 'Research Bot', open_id: changed && config.id === 'account_b' ? 'ou-replaced' : config.botOpenId, activate_status: 1 } };
      else if (path === '/open-apis/im/v1/messages/om-original') {
        assert.equal(new URL(request.url, 'http://localhost').searchParams.get('user_id_type'), 'open_id');
        result = { code: 0, data: { items: [{ ...apiMessage,
          sender: { ...apiMessage.sender, id: `ou-requester-${config.appId}` } }], has_more: false } };
      }
      else if (path === '/open-apis/im/v1/chats/oc-original') result = { code: 0, data: { name: 'Research discussion' } };
      else throw new Error(`Unexpected official operation ${request.method} ${path}`);
    }
    response.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(result));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const httpInstance = { async request(options) {
    const official = new URL(options.url);
    assert.equal(official.origin, 'https://open.feishu.cn');
    const target = new URL(official.pathname, origin);
    for (const [key, value] of Object.entries(options.params ?? {})) target.searchParams.set(key, String(value));
    const response = await fetch(target, { method: options.method, headers: options.headers, signal: options.signal,
      ...(options.data === undefined || ['GET', 'HEAD'].includes(String(options.method).toUpperCase())
        ? {} : { body: JSON.stringify(options.data) }) });
    return response.json();
  }, post(url, data, options) { return this.request({ ...options, method: 'POST', url, data }); },
  get(url, options) { return this.request({ ...options, method: 'GET', url }); } };
  const dispatchers = new Map(), firstEvents = [];
  let inspectOnStart = false;
  const envelope = config => ({ schema: '2.0', header: { event_type: 'im.message.receive_v1',
    event_id: `event-${config.id}`, app_id: config.appId, tenant_key: 'tenant-current' },
  event: { sender: { sender_type: 'user', sender_id: { open_id: `ou-requester-${config.appId}` }, sender_name: 'Researcher',
    tenant_key: 'tenant-current' }, message: { ...original, mentions: mentions.map((mention, index) => ({ ...mention,
      id: { open_id: configs[index] && configs[index].appId !== config.appId
        ? `ou-bot-other-view-${config.appId}` : mention.id.open_id } })) } } });
  class WSClient {
    constructor(options) { this.options = options; }
    async start({ eventDispatcher }) {
      dispatchers.set(this.options.appId, eventDispatcher); this.options.onReady();
      // The first authenticated platform callback arrives as soon as WS is ready.
      if (inspectOnStart) {
        const first = eventDispatcher.invoke(envelope(configs.find(row => row.appId === this.options.appId)));
        firstEvents.push(first); void first.catch(() => undefined);
      }
    }
    getConnectionStatus() { return { state: 'connected' }; }
    close() {}
  }
  const logger = { info() {}, warn() {}, error() {}, debug() {} };
  const controller = new MultiBotDshFeishuController({ registerApp: async () => {}, logger,
    verifyApp: options => verifyFeishuApp({ ...options, httpInstance }),
    credentials: { resolve: async () => ({ value: 'local-fixture-secret' }) },
    configStore: { list: () => structuredClone(configs), getBot: id => structuredClone(configs.find(row => row.id === id)),
      saveBot: async value => { Object.assign(configs.find(row => row.id === value.id), value); return structuredClone(value); } },
    createRuntime: async ({ config, appSecret, acceptExternal }) => new FeishuRuntime({
      lark: { ...lark, defaultHttpInstance: httpInstance, WSClient }, botId: config.id,
      appId: config.appId, botOpenId: config.botOpenId, appSecret, domain: config.domain,
      ownerOpenIds: ['*'], consumerMode: 'external-consumer', acceptExternal,
      harness: { ensureRunning: async () => {} }, state: {}, slashCommands: false, logger,
    }),
  });
  const service = createDeliveryService();
  const disposeAdapter = service.registerAdapter(createDeliveryAdapter({ channel: 'feishu', coreController: controller,
    workspaces: { has: id => configs.some(row => row.id === id) }, stateFor: async () => ({}) }));
  const received = [], sources = [];
  const consumers = [];
  try {
    await controller.initialize();
    inspectOnStart = true;
    for (const config of configs) {
      const account = await service.describeBot(config.id);
      consumers.push(await service.consumeInbound(config.id, { expectedFingerprint: account.account.fingerprint,
        onEvent: async value => {
          received.push(value);
          sources.push(await service.historyChecked(config.id, value.reply, { scope: 'message', limit: 1 },
            { expectedFingerprint: account.account.fingerprint }));
          return { accepted: true };
        } }));
      await firstEvents.at(-1);
      const receiving = await service.describeBot(config.id);
      assert.equal(receiving.connected, true);
      const event = received.at(-1);
      assert.equal(event.tenantId, 'tenant-current');
      assert.equal(event.messageId, original.message_id);
      assert.equal(event.conversation.id, original.chat_id);
      assert.equal(event.fingerprint, account.account.fingerprint);
      assert.equal(event.mentionedAccount, true);
      assert.ok(event.mentions.some(mention => mention.id === `ou-bot-other-view-${config.appId}`));
      const history = sources.at(-1);
      assert.equal(history.source, history.events[0]);
      assert.equal(history.source.messageId, event.messageId);
      assert.equal(history.source.text, event.text);
      assert.equal(history.events[0].tenantId, undefined);
      assert.deepEqual(history.events[0].mentions.filter(mention => mention.idType === 'app_id').map(mention => mention.id),
        configs.map(row => row.appId));
      assert.equal(history.events[0].mentions.find(mention => mention.idType === 'open_id').id, 'ou-human');
      assert.equal(history.events[0].mentionedAccount, true);
    }
    assert.equal(received.length, 2);
    assert.ok(requests.some(row => row.path === '/open-apis/bot/v3/info/'));
    assert.ok(requests.some(row => row.path === '/open-apis/im/v1/messages/om-original'));
    assert.equal(requests.some(row => row.path === '/open-apis/im/v1/messages'), false);
    assert.ok(requests.every(row => row.method === 'GET' || row.path === '/open-apis/auth/v3/tenant_access_token/internal'));
    changed = true;
    await assert.rejects(service.describeBot('account_b'), { code: 'account-changed' });
  } finally {
    for (const dispose of consumers) dispose();
    disposeAdapter(); await controller.close();
    server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
  }
});
