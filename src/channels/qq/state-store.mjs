import { deferredStateAccess, normalizeDeferredState } from '../shared/deferred-state.mjs';
import { createHash, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import fs from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { qqStateError } from './state-error.mjs';

const EMPTY_STATE = Object.freeze({ version: 1, sessions: {}, seenMessageIds: [] });

export function qqContentHash(text) {
  if (typeof text !== 'string') throw new TypeError('QQ source text must be a string');
  return createHash('sha256').update(JSON.stringify(text)).digest('hex');
}

/** Create the same compact source proof from native content or its retained hash. */
export function createQqReplyProof({ fingerprint, route, kind, at, mentionedAccount, contentHash,
  contentHashAlgorithm = 'sha256-json-string-v1', attempts = 0, sequence = 0 }) {
  if (!/^[a-f0-9]{64}$/.test(contentHash ?? '')) throw new TypeError('Invalid QQ content hash');
  if (contentHashAlgorithm !== 'sha256-json-string-v1') throw new TypeError('Unsupported QQ content hash algorithm');
  if (!route || Object.keys(route).some(key => !['messageId', 'conversationId', 'actorId'].includes(key)))
    throw new TypeError('Invalid QQ source route');
  const identity = { route: { messageId: route.messageId, conversationId: route.conversationId, actorId: route.actorId },
    kind, at: new Date(at).toISOString(), mentionedAccount, contentHash, contentHashAlgorithm,
  };
  const proof = { fingerprint, ...identity, attempts, sequence,
    digest: createHash('sha256').update(JSON.stringify(identity)).digest('hex') };
  if (!validReplyProof(proof)) throw new TypeError('Invalid QQ source proof');
  return proof;
}

function validReplyProof(value) {
  return value && /^[a-f0-9]{64}$/.test(value.fingerprint ?? '')
    && /^[a-f0-9]{64}$/.test(value.digest ?? '')
    && ['messageId', 'conversationId', 'actorId'].every(key =>
      typeof value.route?.[key] === 'string' && value.route[key].length > 0)
    && ['dm', 'group'].includes(value.kind)
    && typeof value.mentionedAccount === 'boolean'
    && /^[a-f0-9]{64}$/.test(value.contentHash ?? '')
    && value.contentHashAlgorithm === 'sha256-json-string-v1'
    && Number.isFinite(Date.parse(value.at))
    && Number.isSafeInteger(value.attempts) && value.attempts >= 0
    && Number.isSafeInteger(value.sequence) && value.sequence >= value.attempts;
}

function validGatewaySession(value) {
  return value && /^[a-f0-9]{64}$/.test(value.binding ?? '')
    && typeof value.sessionId === 'string' && value.sessionId.length > 0
    && Number.isSafeInteger(value.lastSeq) && value.lastSeq >= 0;
}

function normalizeState(value) {
  if (!value || typeof value !== 'object') return structuredClone(EMPTY_STATE);
  const sessions = {};
  if (value.sessions && typeof value.sessions === 'object' && !Array.isArray(value.sessions)) {
    for (const [key, sessionId] of Object.entries(value.sessions)) {
      if (typeof key === 'string' && typeof sessionId === 'string' && sessionId) {
        sessions[key] = sessionId;
      }
    }
  }
  return {
    version: 1,
    sessions,
    includeArchivedSessions: value.includeArchivedSessions === true,
    ...(value.deferred ? { deferred: normalizeDeferredState(value.deferred) } : {}),
    ...(validGatewaySession(value.gatewaySession) ? { gatewaySession: {
      binding: value.gatewaySession.binding, sessionId: value.gatewaySession.sessionId,
      lastSeq: value.gatewaySession.lastSeq,
    } } : {}),
    seenMessageIds: Array.isArray(value.seenMessageIds)
      ? value.seenMessageIds.filter((id) => typeof id === 'string').slice(-1_000)
      : [],
  };
}

export class QqStateStore {
  #path;
  #logger;
  #state = structuredClone(EMPTY_STATE);
  #writeQueue = Promise.resolve();
  #deferred = deferredStateAccess(() => this.#state, () => this.#persist());

  constructor(path, { logger = console } = {}) {
    this.#path = path;
    this.#logger = logger;
  }

  async #syncDirectory(path) {
    // Node cannot open directory handles for fsync on Windows. File data is still flushed.
    if (process.platform === 'win32') return;
    const handle = await fs.open(path, 'r');
    try { await handle.sync(); }
    finally { await handle.close(); }
  }

  async #writeDurably(path, bytes, exclusive = false) {
    const directory = dirname(path);
    await fs.mkdir(directory, { recursive: true, mode: 0o700 });
    const boundary = dirname(dirname(this.#path));
    let current = directory;
    while (true) {
      await this.#syncDirectory(current);
      if (current === boundary || current === dirname(current)) break;
      current = dirname(current);
    }
    const temporary = exclusive ? path : `${path}.tmp`;
    const handle = await fs.open(temporary, exclusive ? 'wx' : 'w', 0o600);
    try { await handle.writeFile(bytes); await handle.sync(); }
    finally { await handle.close(); }
    if (!exclusive) await fs.rename(temporary, path);
    await this.#syncDirectory(directory);
  }

  async load() {
    let bytes;
    try {
      bytes = await fs.readFile(this.#path);
    } catch (error) {
      if (error?.code !== 'ENOENT') throw qqStateError('state-read-failed', error);
      this.#state = structuredClone(EMPTY_STATE);
      await this.#persist();
      return this;
    }

    let parsed;
    try {
      parsed = JSON.parse(bytes.toString('utf8'));
    } catch (error) {
      if (!(error instanceof SyntaxError)) throw error;
      const backup = `${this.#path}.corrupt-${Date.now()}-${randomUUID()}`;
      try {
        // Copy the exact bytes before replacing anything. A failed rebuild
        // leaves both the original and this exclusive backup available.
        await this.#writeDurably(backup, bytes, true);
      } catch (cause) {
        throw qqStateError('state-backup-failed', cause);
      }
      this.#state = structuredClone(EMPTY_STATE);
      await this.#persist();
      this.#logger.warn?.(`[dsh-im:qq] state-recovered: ${this.#path}; backup: ${backup}; conversation mappings, message deduplication and deferred deliveries have been reset.`);
      return this;
    }
    this.#state = normalizeState(parsed);
    return this;
  }

  deferredEntries() { return this.#deferred.entries(); }
  putDeferred(entry) { return this.#deferred.put(entry); }
  patchDeferred(id, patch) { return this.#deferred.patch(id, patch); }
  removeDeferred(id) { return this.#deferred.remove(id); }

  #proofPath(fingerprint, messageId) {
    if (!/^[a-f0-9]{64}$/.test(fingerprint ?? '') || typeof messageId !== 'string' || !messageId || messageId.length > 512)
      throw new TypeError('Invalid QQ proof key');
    return join(`${this.#path}.reply-proofs`, fingerprint, `${createHash('sha256').update(messageId).digest('hex')}.json`);
  }

  replyProof(fingerprint, messageId) {
    try {
      const proof = JSON.parse(readFileSync(this.#proofPath(fingerprint, messageId), 'utf8'));
      if (!validReplyProof(proof) || proof.fingerprint !== fingerprint || proof.route.messageId !== messageId)
        throw new TypeError('QQ proof identity differs');
      return proof;
    } catch (error) {
      if (error.code === 'ENOENT') return undefined;
      throw qqStateError('state-read-failed', error);
    }
  }

  replyProofFile(fingerprint, messageId) { return this.#proofPath(fingerprint, messageId); }

  async #updateReplyProof(fingerprint, messageId, update) {
    const operation = this.#writeQueue.then(async () => {
      const previous = this.replyProof(fingerprint, messageId);
      const next = update(previous);
      if (next === undefined) return previous;
      const path = this.#proofPath(fingerprint, messageId);
      try {
        if (next === null) {
          await fs.rm(path, { force: true });
          let directory = dirname(path);
          while (true) {
            try { await this.#syncDirectory(directory); break; }
            catch (error) {
              if (error.code !== 'ENOENT' || directory === dirname(directory)) throw error;
              directory = dirname(directory);
            }
          }
          return undefined;
        }
        if (!validReplyProof(next)) throw new TypeError('Invalid QQ reply proof');
        await this.#writeDurably(path, `${JSON.stringify(next)}\n`);
        return next;
      } catch (error) { throw qqStateError('state-write-failed', error); }
    });
    this.#writeQueue = operation.then(() => undefined, () => undefined);
    return operation;
  }

  async putReplyProof(value) {
    if (!validReplyProof(value)) throw new TypeError('Invalid QQ reply proof');
    await this.#updateReplyProof(value.fingerprint, value.route.messageId, prior => {
      if (prior) {
        if (prior.digest !== value.digest)
          throw Object.assign(new Error('QQ source changed'), { code: 'stale-route' });
        return prior;
      }
      return structuredClone(value);
    });
  }

  async reserveReplyAttempt(fingerprint, messageId, limit, sequence) {
    const proof = await this.#updateReplyProof(fingerprint, messageId, proof => {
      if (!proof) throw Object.assign(new Error('QQ source unavailable'), { code: 'source-not-found' });
      if (proof.attempts >= limit) throw Object.assign(new Error('QQ reply budget exhausted'), { code: 'reply-limit-exceeded' });
      proof.attempts++;
      if (sequence === undefined) sequence = ++proof.sequence;
      return proof;
    });
    return { attempts: proof.attempts, sequence };
  }

  async refundReplyAttempt(fingerprint, messageId) {
    await this.#updateReplyProof(fingerprint, messageId, proof => {
      if (proof) proof.attempts = Math.max(0, proof.attempts - 1);
      return proof;
    });
  }

  async releaseReplyProof(fingerprint, route) {
    let retainUntil;
    let retained = false;
    await this.#updateReplyProof(fingerprint, route.messageId, proof => {
      if (!proof) return null;
      if (['messageId', 'conversationId', 'actorId'].some(key => proof.route[key] !== route[key]))
        throw Object.assign(new Error('QQ source changed'), { code: 'stale-route' });
      retainUntil = Date.parse(proof.at) + (proof.kind === 'dm' ? 3600000 : 300000);
      retained = (proof.attempts > 0 || proof.sequence > 0) && retainUntil > Date.now();
      return retained ? { ...proof, completed: true } : null;
    });
    return { released: true, ...(retained ? { retainUntil } : {}) };
  }

  gatewaySession(binding) {
    const value = this.#state.gatewaySession;
    return value?.binding === binding ? { sessionId: value.sessionId, lastSeq: value.lastSeq } : null;
  }

  async setGatewaySession(binding, session) {
    if (session !== null && !validGatewaySession({ binding, ...session }))
      throw new TypeError('Invalid QQ gateway session');
    await this.#persist(state => {
      if (session === null) delete state.gatewaySession;
      else state.gatewaySession = { binding, sessionId: session.sessionId, lastSeq: session.lastSeq };
    }, 'gatewaySession');
  }

  includesArchivedSessions() { return this.#state.includeArchivedSessions === true; }

  async setIncludeArchivedSessions(include) {
    this.#state.includeArchivedSessions = include === true;
    await this.#persist();
  }

  sessionFor(key) {
    return this.#state.sessions[key] ?? null;
  }

  async setSession(key, sessionId) {
    this.#state.sessions[key] = sessionId;
    await this.#persist();
  }

  async clearSession(key) {
    delete this.#state.sessions[key];
    await this.#persist();
  }

  async clearSessions() {
    this.#state.sessions = {};
    await this.#persist();
  }

  hasSeen(messageId) {
    return this.#state.seenMessageIds.includes(messageId);
  }

  async markSeen(messageId) {
    if (this.hasSeen(messageId)) return;
    this.#state.seenMessageIds.push(messageId);
    if (this.#state.seenMessageIds.length > 1_000) {
      this.#state.seenMessageIds.splice(0, this.#state.seenMessageIds.length - 1_000);
    }
    await this.#persist();
  }

  snapshot() {
    return structuredClone(this.#state);
  }

  async remove() {
    await this.#writeQueue;
    await fs.rm(`${this.#path}.reply-proofs`, { recursive: true, force: true });
    try {
      await fs.unlink(this.#path);
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error;
    }
    await this.#syncDirectory(dirname(this.#path));
    this.#state = structuredClone(EMPTY_STATE);
  }

  async #persist(update, field) {
    let result;
    let writing = false;
    const operation = this.#writeQueue.then(async () => {
      const next = update ? { ...this.#state, [field]: structuredClone(this.#state[field]) } : this.#state;
      result = update?.(next);
      const snapshot = `${JSON.stringify(next, null, 2)}\n`;
      writing = true;
      await this.#writeDurably(this.#path, snapshot);
      if (update) {
        if (Object.hasOwn(next, field)) this.#state[field] = next[field];
        else delete this.#state[field];
      }
    });
    this.#writeQueue = operation.then(() => undefined, () => undefined);
    await operation.catch(error => {
      if (!writing) throw error;
      throw qqStateError('state-write-failed', error);
    });
    return result;
  }
}
