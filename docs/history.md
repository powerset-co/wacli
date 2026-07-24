# history

Read when: trying to fetch older messages for a known chat.

`wacli history` inspects local archive coverage and can send on-demand history sync requests to the primary device. Backfill is best-effort and depends on the phone being online and WhatsApp returning older messages.

## Commands

```bash
wacli history coverage [--query TEXT] [--kind KIND] [--include-blocked] [--only-actionable]
wacli history fill --dry-run [--query TEXT] [--kind KIND] [--limit 100]
wacli history backfill --chat JID [--count 50] [--requests N] [--wait 1m] [--idle-exit 5s] [--events]
wacli history backfill-batch --chat JID --chat JID [--batch-size 10] [--max-inflight 10] [--wait 10s] [--timeout-backoff 1m]
```

## Coverage and planning

- `history coverage` reads only the local `wacli.db` store.
- `ready` chats have at least one local message, so `history backfill` has an anchor.
- `blocked` / `no_local_anchor` chats have no local message yet; run `wacli sync` first.
- `history fill --dry-run` lists matching ready chats that would be selected for a future multi-chat fill workflow. It does not connect to WhatsApp or write state.

## Limits

- `--count` defaults to 50 and must be at most 500.
- `--requests` defaults to 1 and must be at most 100.
- Requests are per chat.
- The anchor is the oldest locally stored message in that chat.
- Automatic initial history-sync blob downloads are disabled during backfill; only on-demand responses are processed.
- `--events` emits NDJSON request/response/stop lifecycle events on stderr.
- `backfill-batch` keeps one connection open, bounds simultaneous requests with
  `--max-inflight`, and paces groups with `--batch-size` and `--batch-delay`.
- If a whole batch receives no protocol responses after identity fallback,
  `--timeout-backoff` pauses one minute by default before the next batch.
- For DMs, batch backfill remembers whether the phone-number JID (`pn`) or
  hidden account identifier (`lid`) last produced a useful response. The
  preference stays inside the local wacli database. A cached route is tried
  first; a timeout falls back to the other mapped identity, while an unknown
  chat may also fall back after an empty/no-growth response. Chat storage keys
  do not change.

## Examples

```bash
wacli history coverage --include-blocked
wacli history coverage --query family --only-actionable
wacli history fill --dry-run --kind group --limit 20
wacli history backfill --chat 1234567890@s.whatsapp.net --requests 10 --count 50
wacli history backfill --chat 123456789@g.us --requests 3 --wait 90s
wacli history backfill-batch \
  --chat 1234567890@s.whatsapp.net \
  --chat 15551234567@s.whatsapp.net \
  --count 500 --requests 10 --batch-size 10 --max-inflight 10
```
