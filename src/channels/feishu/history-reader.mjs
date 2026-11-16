import { readNearbyHistory } from './nearby-history.mjs';
import { normalizeExternalText } from './external-consumer.mjs';

function refuse(code) { throw Object.assign(new Error(code), { code }); }
function identifier(value) {
  return typeof value === 'string' && value.length > 0 && value.length <= 512;
}

function check(response) {
  if ([230027, 99991672, 99991679].includes(response?.code)) refuse('history-permission-denied');
  if (response?.code) refuse('history-unavailable');
}

// Application-defined checked read: destination is derived from an authenticated source.
// Lark has Chat/time-range listing and Thread listing, not a native around-message API.
export async function readExternalHistory(client, identity, route, query, signal) {
  if (!route || !identifier(route.messageId) || !identifier(route.conversationId)
    || !identifier(route.actorId) || ['threadId', 'rootId', 'parentId'].some(key => route[key] !== undefined && !identifier(route[key])) || !['message', 'group', 'nearby', 'thread'].includes(query?.scope)
    || !Number.isInteger(query.limit) || query.limit < 1 || query.limit > 20
    || (query.cursor !== undefined && (typeof query.cursor !== 'string' || !query.cursor || query.cursor.length > 4096)))
    refuse('bad-request');
  if (query.scope === 'message' && (query.limit !== 1 || query.cursor !== undefined
    || query.beforeCount !== undefined || query.afterCount !== undefined)) refuse('bad-request');
  if (query.scope === 'nearby' && [query.beforeCount ?? 10, query.afterCount ?? 5]
    .some(value => !Number.isInteger(value) || value < 0 || value > 20)) refuse('bad-request');
  signal?.throwIfAborted();
  const reference = await client.im.v1.message.get({ path: { message_id: route.messageId }, params: { with_sender_name: true, user_id_type: 'open_id' } }, { signal });
  signal?.throwIfAborted();
  check(reference);
  const source = (Array.isArray(reference?.data?.items) ? reference.data.items : []).find(item => item?.message_id === route.messageId);
  if (!source || source.deleted || source.chat_id !== route.conversationId
    || source.sender?.sender_type !== 'user' || source.sender?.id_type !== 'open_id'
    || source.sender?.id !== route.actorId || (source.thread_id || undefined) !== route.threadId
    || (source.root_id || undefined) !== route.rootId || (source.parent_id || undefined) !== route.parentId)
    refuse('stale-route');
  if (query.scope === 'thread' && !route.threadId) refuse('thread-unavailable');
  signal?.throwIfAborted();
  const seconds = Math.floor(Number(source.create_time) / 1000);
  if (!Number.isFinite(seconds) || seconds <= 0) refuse('stale-route');
  // Explicit user_id_type preserves Human open_id and returns Bot app_id.
  // https://open.feishu.cn/document/server-docs/im-v1/message/get.md
  // Bot open_id equality across receiving apps is not an identity contract.
  const normalize = item => normalizeExternalText({ event_id: `history:${item.message_id}`,
    sender: { sender_type: 'user', sender_id: { open_id: item.sender.id }, sender_name: item.sender.sender_name,
      ...(item.sender.tenant_key === undefined ? {} : { tenant_key: item.sender.tenant_key }) },
    message: { ...item, chat_type: 'group', message_type: item.msg_type, content: item.body?.content,
      mentions: (item.mentions ?? []).map(mention => ({ ...mention,
        id: { [mention?.id_type === 'app_id' ? 'app_id' : 'open_id']: mention?.id } })) },
  }, identity);
  if (query.scope === 'message') {
    if (source.updated === true) refuse('stale-route');
    if (source.updated !== false) refuse('source-unavailable');
    let anchor;
    try { anchor = normalize(source); }
    catch (error) { if (error?.code !== 'invalid-inbound') throw error; refuse('source-unavailable'); }
    if (!anchor) refuse('source-unavailable');
    return { version: 1, scope: 'message', source: anchor, events: [anchor], omitted: 0,
      hasMore: false, coverage: 'provider-verified-source' };
  }
  const readPage = async params => {
    const response = await client.im.v1.message.list({ params }, { signal });
    signal?.throwIfAborted();
    check(response);
    const items = response?.data?.items;
    if (!Array.isArray(items) || items.length > query.limit || typeof response.data.has_more !== 'boolean')
      refuse('history-unavailable');
    const events = [];
    let omitted = 0;
    for (const item of items) {
      if (!item || item.chat_id !== route.conversationId
        || (query.scope === 'thread' && item.thread_id !== route.threadId)) refuse('untrusted-source');
      if (item.deleted || item.msg_type !== 'text' || item.sender?.sender_type !== 'user'
        || item.sender?.id_type !== 'open_id') { ++omitted; continue; }
      if (item.mentions !== undefined && (!Array.isArray(item.mentions) || item.mentions.length > 100)) { ++omitted; continue; }
      let event;
      try { event = normalize(item); } catch (error) {
        if (error?.code !== 'invalid-inbound') throw error;
        ++omitted; continue;
      }
      if (!event) { ++omitted; continue; }
      events.push(event);
    }
    const nextCursor = response.data.has_more ? response.data.page_token : undefined;
    if (response.data.has_more && (typeof nextCursor !== 'string' || !nextCursor || nextCursor.length > 4096))
      refuse('history-unavailable');
    return {events, omitted, hasMore:response.data.has_more, ...(nextCursor ? {nextCursor} : {})};
  };
  if (query.scope === 'nearby') return readNearbyHistory(readPage, route, query, Number(source.create_time));
  if (query.beforeCount !== undefined || query.afterCount !== undefined) refuse('bad-request');
  const page = await readPage({container_id_type:query.scope === 'thread' ? 'thread' : 'chat',
    container_id:query.scope === 'thread' ? route.threadId : route.conversationId,
    sort_type:'ByCreateTimeDesc', page_size:query.limit, with_sender_name:true,
    ...(query.cursor === undefined ? {} : {page_token:query.cursor})});
  return {version:1, scope:query.scope, ...page, coverage:'provider-visible-human-text'};
}
