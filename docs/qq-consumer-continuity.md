# QQ consumer continuity across controlled updates

An exclusive QQ consumer keeps accepted work bound to its original message when
the Provider restarts. The existing account state stores compact reply proofs,
the next native `msg_seq`, and any attempted reply budget. Message text remains
with the consumer. Source proofs contain a hash of that text and the original
account, route, timestamp and reply eligibility.

Proofs are written before inbound acceptance. Reply attempts are reserved before
the native POST. An uncertain send keeps its spent attempt across restart. A
definite rate-limit refusal releases the attempt and reuses the same sequence
for the existing bounded retry. The normal native reply window and reply budget
continue to apply.

Proof files are looked up by their known account/message key. The bounded memory
cache does not evict unfinished work from disk. Files are flushed before atomic
rename. On systems where Node supports directory handles, the containing
directories are synchronized too and a synchronization failure rejects the
operation. Windows retains file flushing and atomic replacement because Node
cannot fsync a directory there. This contract covers completed writes and
controlled process replacement, not a power-loss guarantee on every filesystem.

## Completing a source

The consumer can return `{ accepted: true, complete: true }` when no work remains,
or use the optional `sourceReleaseVersion: 1` operation:

```js
const result = await ctx.dshIm.releaseSource(botId, event.reply, {
  expectedFingerprint: event.fingerprint,
  signal,
});
```

The result is `{ released: true }`. A source with a previous native attempt keeps
its budget through the remaining reply window and returns `retainUntil` in
milliseconds since the epoch. The owner calls the same release operation again
after that time. Redelivery cannot reset a spent budget. Releasing a source does
not erase the consumer's own work or message history.

## Updating the receiver

The optional `inboundDrainVersion: 1` methods use the current verified account and
exclusive consumer lease:

1. Call `quiesceInbound(botId, { expectedFingerprint, signal })` while the lease is
   still active. It stops intake, waits for callbacks and canonical acceptance,
   then saves the native session/cursor. It returns
   `{ stopped: true, resumeCursor: boolean }`.
2. Dispose the old lease after quiescing. Provider shutdown follows the same
   ordering before unregistering the public delivery adapter.
3. Call `resumeInbound` to verify the same account, then register the new consumer
   with `consumeInbound`. Verification can return
   `{ connected: false, accountVerified: true }` while reception waits for its
   consumer. A reconnect does not silently reopen a quiesced receiver.

The saved native session is bound to the account and credential. A rejected
inbound callback prevents a resumable cursor from being recorded. The platform
controls replay availability and retention. A saved cursor does not promise
recovery after abrupt termination or platform replay expiry.
