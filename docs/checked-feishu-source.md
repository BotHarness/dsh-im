# Checked original Feishu message

An exclusive Feishu/Lark consumer can inspect an original Human group-text
message before accepting it:

```js
const source = await ctx.dshIm.historyChecked(
  botId,
  event.reply,
  { scope: 'message', limit: 1 },
  { expectedFingerprint: event.fingerprint, signal },
);
```

This uses the existing `history-text-checked` capability and current consumer
lease. It performs one native message GET with `user_id_type: 'open_id'` and no
history-list request. The result has `scope: 'message'`,
`coverage: 'provider-verified-source'`, `source`, and `events: [source]`.
Pagination and nearby-history options are rejected for this scope.

The provider checks the message, conversation, actor and thread/reply identities
against the original route. Edited messages are rejected as `stale-route`.
Missing edit status or unsupported source content returns `source-unavailable`.
The current account, runtime and consumer lease are checked across the read,
including a read made inside an inbound acceptance callback.

The [native GET contract](https://open.feishu.cn/document/server-docs/im-v1/message/get.md)
returns Human mention IDs as `open_id` and Bot mention IDs as `app_id` when
`user_id_type` is specified. Normalized mentions preserve that namespace in
`idType`, alongside their `id` and placeholder `key`. Bot `app_id` mentions have
`kind: 'bot'`. A mention of the receiving application matches its verified
application ID. An `open_id` from another receiving application's view is not
used as an application ID.

Live inbound events preserve the authenticated application ID and an explicitly
provided tenant ID. Mention names and typed metadata remain optional. Missing
tenant data in a GET response is not filled from another event or a cached
account. The consumer remains responsible for relating the returned source to
its accepted event and for any user authorization based on that source.

Existing `group`, `thread`, and `nearby` history scopes keep their pagination and
Human-text coverage. List requests use the native list parameters, without the
GET-specific `user_id_type` option.
