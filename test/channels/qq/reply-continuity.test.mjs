import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { mkdtemp, mkdir, readFile, rm } from 'node:fs/promises';
import fs from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { QQBot } from '@tencent-connect/qqbot-nodejs';
import { QqExternalConsumer, verifiedQqAccount } from '../../../src/channels/qq/external-consumer.mjs';
import { QqStateStore } from '../../../src/channels/qq/state-store.mjs';

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'qq-reply-continuity-'));
  const path = join(directory, 'state.json');
  const posts = [];
  let uncertain = false;
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    if (request.url.includes('getAppAccessToken')) {
      response.setHeader('content-type', 'application/json');
      response.end(JSON.stringify({ access_token: 'fixture-token', expires_in: 7200 }));
      return;
    }
    posts.push({ path: request.url, body: JSON.parse(body) });
    if (uncertain) { request.socket.destroy(); return; }
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({ id: `reply-${posts.length}` }));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  });
  const url = `http://127.0.0.1:${server.address().port}`;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (input, options) => {
    assert.equal(new URL(String(input)).origin, url, 'HTTP fixture must stay on loopback');
    return originalFetch(input, options);
  };
  t.after(() => { globalThis.fetch = originalFetch; });
  const account = verifiedQqAccount('application-one', { id: 'native-bot', bot: true });
  const signal = new AbortController().signal;
  let accepted = 0;
  const consumer = state => new QqExternalConsumer({
    bot: new QQBot({ appId: account.appId, appSecret: 'fixture-secret',
      baseUrl: url, tokenBaseUrl: url, logger: { debug() {}, info() {}, warn() {}, error() {} } }),
    botId: 'fixture-bot', account, state,
    accept: async () => { accepted++; return { accepted: true }; },
  });
  const message = {
    kind: 'group', rawEventType: 'GROUP_AT_MESSAGE_CREATE', messageId: 'original-message',
    senderId: 'original-member', groupOpenid: 'original-group', content: 'private canonical body',
    timestamp: new Date().toISOString(),
    replyTarget: { scope: 'group', targetId: 'original-group', msgId: 'original-message' },
  };
  const route = { messageId: message.messageId, actorId: message.senderId, conversationId: message.groupOpenid };
  return { path, posts, account, signal, consumer, message, route,
    accepted: () => accepted, uncertain: () => { uncertain = true; } };
}

test('original QQ source and send sequence survive a process-local consumer replacement', async t => {
  const fx = await fixture(t);
  const firstStore = await new QqStateStore(fx.path).load();
  const first = fx.consumer(firstStore);
  await first.accept(fx.message, fx.signal);
  await first.reply(fx.route, 'first answer', { signal: fx.signal, beforeSend: () => true });
  const restoredStore = await new QqStateStore(fx.path).load();
  const restored = fx.consumer(restoredStore);
  assert.deepEqual(restored.qualify(fx.route, fx.signal), fx.route);
  const result = await restored.reply(fx.route, 'later answer', { signal: fx.signal, receipt: true, beforeSend: () => true });
  assert.equal(result.receipt.messageId, 'reply-2');
  assert.deepEqual(fx.posts.map(post => post.body.msg_id), ['original-message', 'original-message']);
  assert.deepEqual(fx.posts.map(post => post.body.msg_seq), [1, 2]);
  assert.equal(fx.accepted(), 1);
  assert.equal((await readFile(fx.path, 'utf8')).includes('private canonical body'), false);
  assert.equal(restoredStore.replyProof(fx.account.fingerprint, fx.route.messageId).attempts, 2);
  const other = new QqExternalConsumer({ bot: {}, botId: 'fixture-bot', state: restoredStore,
    account: verifiedQqAccount('other-application', { id: 'other-bot', bot: true }), accept: async () => ({ accepted: true }) });
  assert.throws(() => other.qualify(fx.route, fx.signal), { code: 'source-not-found' });
});

test('a failed proof write prevents canonical admission and native sending', async t => {
  const fx = await fixture(t);
  const store = await new QqStateStore(fx.path).load();
  const consumer = fx.consumer(store);
  await mkdir(`${fx.path}.reply-proofs/${fx.account.fingerprint}/${createHash('sha256').update(fx.route.messageId).digest('hex')}.json.tmp`, { recursive: true });
  await assert.rejects(() => consumer.accept(fx.message, fx.signal), { code: 'state-write-failed' });
  assert.equal(fx.accepted(), 0);
  assert.throws(() => consumer.qualify(fx.route, fx.signal), { code: 'source-not-found' });
  assert.throws(() => fx.consumer(store).qualify(fx.route, fx.signal), { code: 'source-not-found' });
  assert.equal(fx.posts.length, 0);
});

test('an uncertain real HTTP send occurs once and its attempted budget survives restart', async t => {
  const fx = await fixture(t);
  const store = await new QqStateStore(fx.path).load();
  const consumer = fx.consumer(store);
  await consumer.accept(fx.message, fx.signal);
  fx.uncertain();
  await assert.rejects(() => consumer.reply(fx.route, 'answer', { signal: fx.signal, beforeSend: () => true }),
    { code: 'reply-result-unknown' });
  assert.equal(fx.posts.length, 1);
  const restored = await new QqStateStore(fx.path).load();
  assert.equal(restored.replyProof(fx.account.fingerprint, fx.route.messageId).attempts, 1);
  assert.equal(restored.replyProof(fx.account.fingerprint, fx.route.messageId).sequence, 1);
  assert.deepEqual(fx.consumer(restored).qualify(fx.route, fx.signal), fx.route);
  assert.equal(fx.posts.length, 1);
});

test('concurrent reply reservations allocate distinct durable sequences within the native budget', async t => {
  const fx = await fixture(t);
  const store = await new QqStateStore(fx.path).load();
  await fx.consumer(store).accept(fx.message, fx.signal);
  const outcomes = await Promise.allSettled(Array.from({ length: 6 }, () =>
    store.reserveReplyAttempt(fx.account.fingerprint, fx.route.messageId, 5)));
  assert.deepEqual(outcomes.filter(value => value.status === 'fulfilled').map(value => value.value.sequence), [1, 2, 3, 4, 5]);
  assert.equal(outcomes[5].reason.code, 'reply-limit-exceeded');
  const restored = await new QqStateStore(fx.path).load();
  assert.throws(() => fx.consumer(restored).qualify(fx.route, fx.signal), { code: 'reply-limit-exceeded' });
  assert.equal(fx.posts.length, 0);
});

test('more than two thousand later messages cannot evict an unfinished original reply after restart', async t => {
  const fx = await fixture(t);
  const state = await new QqStateStore(fx.path).load();
  const consumer = fx.consumer(state);
  await consumer.accept(fx.message, fx.signal);
  for (let index = 0; index < 2001; index++) {
    const id = `later-${index}`;
    await consumer.accept({ ...fx.message, messageId: id,
      replyTarget: { ...fx.message.replyTarget, msgId: id } }, fx.signal);
  }
  assert.deepEqual(consumer.qualify(fx.route, fx.signal), fx.route);
  const restored = fx.consumer(await new QqStateStore(fx.path).load());
  await restored.reply(fx.route, 'the original long task completed', { signal: fx.signal, beforeSend: () => true });
  assert.equal(fx.posts.length, 1);
  assert.equal(fx.posts[0].body.msg_id, fx.route.messageId);
  assert.equal(JSON.parse(await readFile(fx.path, 'utf8')).replyProofs, undefined);
});

test('completed sources keep their native budget through redelivery and expire on the original release call', async t => {
  const fx = await fixture(t);
  const state = await new QqStateStore(fx.path).load();
  let consumer = fx.consumer(state);
  await consumer.accept(fx.message, fx.signal);
  await consumer.reply(fx.route, 'answer', { signal: fx.signal, beforeSend: () => true });
  const released = await consumer.release(fx.route, fx.signal);
  assert.ok(released.retainUntil > Date.now());
  consumer = fx.consumer(await new QqStateStore(fx.path).load());
  await consumer.accept(fx.message, fx.signal);
  assert.equal(state.replyProof(fx.account.fingerprint, fx.route.messageId).attempts, 1);
  const clock = Date.now;
  try {
    Date.now = () => released.retainUntil + 1;
    assert.deepEqual(await consumer.release(fx.route, fx.signal), { released: true });
    assert.equal(state.replyProof(fx.account.fingerprint, fx.route.messageId), undefined);
  } finally { Date.now = clock; }
});

test('explicitly completed intake without any native attempt releases its proof immediately', async t => {
  const fx = await fixture(t);
  const state = await new QqStateStore(fx.path).load();
  const consumer = new QqExternalConsumer({ bot: {}, botId: 'fixture-bot', account: fx.account, state,
    accept: async () => ({ accepted: true, complete: true }) });
  await consumer.accept(fx.message, fx.signal);
  assert.equal(state.replyProof(fx.account.fingerprint, fx.route.messageId), undefined);
  await consumer.accept(fx.message, fx.signal);
  assert.equal(state.replyProof(fx.account.fingerprint, fx.route.messageId), undefined);
  assert.equal(fx.posts.length, 0);
});

test('source and attempt writes flush file data before rename and sync supported directories', async t => {
  const fx = await fixture(t);
  const state = await new QqStateStore(fx.path).load();
  const events = [];
  const originalOpen = fs.open, originalRename = fs.rename;
  t.mock.method(fs, 'open', async (...args) => {
    const handle = await originalOpen(...args);
    const originalSync = handle.sync.bind(handle);
    handle.sync = async () => { await originalSync(); events.push(['sync', String(args[0])]); };
    return handle;
  });
  t.mock.method(fs, 'rename', async (from, to) => { await originalRename(from, to); events.push(['rename', to]); });
  await fx.consumer(state).accept(fx.message, fx.signal);
  const path = state.replyProofFile(fx.account.fingerprint, fx.route.messageId);
  const renamed = events.findIndex(event => event[0] === 'rename' && event[1] === path);
  const synced = events.findIndex(event => event[0] === 'sync' && event[1] === `${path}.tmp`);
  assert.ok(synced >= 0 && synced < renamed);
  if (process.platform !== 'win32') {
    assert.ok(events.slice(renamed + 1).some(event => event[0] === 'sync' && event[1] === dirname(path)));
    assert.ok(events.some(event => event[0] === 'sync' && event[1] === `${fx.path}.reply-proofs`));
  }
  events.length = 0;
  await state.reserveReplyAttempt(fx.account.fingerprint, fx.route.messageId, 5);
  const expected = process.platform === 'win32' ? ['sync', 'rename'] : ['sync', 'rename', 'sync'];
  assert.deepEqual(events.map(event => event[0]).slice(-expected.length), expected);
});

test('a directory synchronization failure refuses admission and the retry repeats the durability barrier',
  { skip: process.platform === 'win32' }, async t => {
  const fx = await fixture(t);
  const state = await new QqStateStore(fx.path).load();
  const path = state.replyProofFile(fx.account.fingerprint, fx.route.messageId);
  const directory = dirname(path);
  const originalOpen = fs.open;
  let fail = true, completedSyncs = 0;
  t.mock.method(fs, 'open', async (...args) => {
    const handle = await originalOpen(...args);
    if (String(args[0]) === directory) {
      const originalSync = handle.sync.bind(handle);
      handle.sync = async () => {
        const exists = await fs.stat(path).then(() => true, () => false);
        if (exists && fail) { fail = false; throw new Error('fixture directory sync failure'); }
        await originalSync(); if (exists) completedSyncs++;
      };
    }
    return handle;
  });
  await assert.rejects(() => fx.consumer(state).accept(fx.message, fx.signal), { code: 'state-write-failed' });
  assert.equal(fx.accepted(), 0);
  await fx.consumer(state).accept(fx.message, fx.signal);
  assert.equal(fx.accepted(), 1);
  assert.ok(completedSyncs > 0);
});

test('ancestor creation and completed-proof deletion retry their failed directory synchronization',
  { skip: process.platform === 'win32' }, async t => {
  const fx = await fixture(t);
  const state = await new QqStateStore(fx.path).load();
  const path = state.replyProofFile(fx.account.fingerprint, fx.route.messageId);
  const directory = dirname(path);
  const originalOpen = fs.open;
  let failAncestor = true, failRemoval = false, ancestorSynced = false, removalSynced = false;
  t.mock.method(fs, 'open', async (...args) => {
    const handle = await originalOpen(...args), sync = handle.sync.bind(handle);
    handle.sync = async () => {
      const name = String(args[0]);
      if (name === `${fx.path}.reply-proofs` && failAncestor) {
        failAncestor = false; throw new Error('fixture ancestor sync failure');
      }
      if (name === directory && failRemoval) {
        failRemoval = false; throw new Error('fixture deletion sync failure');
      }
      await sync();
      if (name === `${fx.path}.reply-proofs`) ancestorSynced = true;
      if (name === directory && !(await fs.stat(path).then(() => true, () => false))) removalSynced = true;
    };
    return handle;
  });
  const consumer = fx.consumer(state);
  await assert.rejects(() => consumer.accept(fx.message, fx.signal), { code: 'state-write-failed' });
  assert.equal(fx.accepted(), 0);
  await consumer.accept(fx.message, fx.signal); assert.equal(ancestorSynced, true);
  failRemoval = true; removalSynced = false;
  await assert.rejects(() => consumer.release(fx.route, fx.signal), { code: 'state-write-failed' });
  assert.deepEqual(await consumer.release(fx.route, fx.signal), { released: true });
  assert.equal(removalSynced, true);
});
