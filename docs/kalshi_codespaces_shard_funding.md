# Operator-run Kalshi shard funding from Codespaces

`kalshi_shard_admin.py` defaults to authenticated **read-only** diagnostics.
It can prepare a one-time transfer of **100% of the currently available cash on
shard 0** to the exchange reported by the active KXBTC15M market. It does not
transfer positions, withdraw money, place orders, reset a breaker, or start an
Actions worker. Current market metadata, not a hard-coded crypto shard, determines
the destination.

The transfer and optional recurring allocation commands below are for the account
owner to execute. Neither has been executed against a real account during
development. Only read-only account diagnostics and offline mocked tests were run.

## 1. Load the reviewed code and secrets

While this remains draft PR #87, open this repository in a Codespace and run:

```bash
gh pr checkout 87
git pull --ff-only
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements_kalshi_settlement_trader.txt
python -m unittest tests.test_kalshi_shard_admin -q
```

Use GitHub **Settings → Codespaces → Codespaces secrets** to configure:

- `KALSHI_PROD_API_KEY` (or `KALSHI_API_KEY_ID`)
- `KALSHI_PRIVATE_KEY` (the complete multiline RSA private key)

Grant these Codespaces secrets access to this repository, then stop/restart the
Codespace to load them. GitHub Actions environment secrets do not automatically
become Codespaces development secrets. Do not put credentials in Python files,
terminal commands, notebook cells, screenshots, or source control. An existing
private local PEM can alternatively be referenced with `KALSHI_PEM_PATH`.
Treat a private key pasted into a chat as exposed: replace it privately in Kalshi
and store the replacement only through the appropriate secret settings.
[GitHub Codespaces secret instructions](https://docs.github.com/en/codespaces/managing-your-codespaces/managing-your-account-specific-secrets-for-github-codespaces).

## 2. Inspect — no transfers or orders

```bash
python kalshi_shard_admin.py status
python kalshi_shard_admin.py transfers
python kalshi_shard_admin.py transfer-all
```

The status reports authentication, the **current key's** write scope, whether it
is an unrestricted primary-account key, location-attestation expiry when supplied,
market ticker/shard, total cash, source/destination available cash and existing
automatic allocation. It never prints the key ID, private key or signed headers.
`can_submit_orders=false` describes this admin utility: it has no order endpoint.
Successful admin preflight does not prove the bot is ready or clear its breaker.
The read-only `transfers` command also checks nonterminal transfers individually
by ID, prints unresolved IDs/amounts/routes/timestamps, and never submits a transfer.
Pending outgoing/event-contract shard transfers and unknown transfer states block
new writes. Verified **incoming `margined` → `event_contract` credits** are instead
reported as warnings: they cannot debit event-contract collateral and are not a
duplicate of the intended shard transfer. Their amounts are never included in
available cash. They remain unresolved; this distinction does not pretend that
Kalshi completed them. Age alone never makes a pending transfer safe to ignore.
Resolve blocking transfers through Kalshi rather than deleting a journal or
bypassing the check. The transfer preview runs the full read-only account preflight.

The default series is `KXBTC15M`; `--ticker` accepts an explicitly API-discovered
market instead of discovering the current active market. Do not use an old market
from another shard as the target for a current crypto strategy.

### If you receive HTTP 401 / authentication_error

Do not keep attempting the transfer. Update this branch and run the read-only
authentication diagnostic from the repository directory:

```bash
git pull --ff-only
source .venv/bin/activate
python kalshi_shard_admin.py auth-check
```

The diagnostic prints only known variable names and presence/format flags, the
production API host, SDK version, local RSA signing self-check, and the result of
one authenticated balance GET. It does not print key IDs, private keys, signed
headers, raw API error bodies, or your balance. It never submits a transfer/order
or creates an operation journal.

Diagnostic version 2 also classifies recognized markers in Kalshi error
`code`/`message`/`details` fields. `server_reasons` can distinguish a rejected
signature, an unrecognized/expired key, a timestamp issue, missing headers, or an
access/scope restriction **when the response supplies recognizable evidence**.
Only fixed classifications and validated request identifiers are emitted, never
arbitrary server text. An empty list means the reason remains unknown. Header
presence flags reflect what this client supplied, not proof of what a downstream
gateway received. UTC time and request identifiers help Kalshi investigate.

- `key_id_alias_conflict=true`: both `KALSHI_API_KEY_ID` and
  `KALSHI_PROD_API_KEY` are set to different values. Older code silently chose
  the first; current code refuses to guess. Keep only the current production
  ID matching your private key, or make both aliases identical.
- Missing credentials: set **Codespaces** secrets with repository access. These
  are not GitHub Actions environment secrets. Stop and start the Codespace after
  adding/changing them; a new shell alone may still inherit the old environment.
- `signer_self_check=PASS` followed by a remote 401: the PEM parses and the SDK
  signs the expected path with millisecond timestamps, but Kalshi still rejects
  the request. Check that ID and PEM belong to the **same current production
  key**, that the key was not revoked, and that it is not a demo key. A local
  signature test cannot verify the public key registered for an API key ID.
- Surrounding whitespace and literal `\n`/`\r\n` PEM line endings are normalized
  in memory. Quoted, truncated, encrypted, or non-RSA PEMs fail with a sanitized
  message. Store the actual multiline PEM without surrounding quote characters.
- A large `approx_http_date_clock_offset_seconds` is a clock diagnostic only:
  HTTP Date may be approximate/cached. The tool never changes the clock or
  automatically adjusts signing timestamps.

The program uses the production host only. Kalshi production/demo credentials
are not interchangeable. Authentication failures are not fixed by funding a
shard, changing strategy parameters, or resetting a trading breaker.
[Kalshi request signing](https://docs.kalshi.com/getting_started/api_keys),
[Kalshi environments](https://docs.kalshi.com/getting_started/api_environments).

Only after `AUTH_REMOTE_CHECK` reports `PASS`, run `status` and the read-only
transfer preview again. The remote authentication check does not establish write
permission or transfer readiness by itself; the complete preflight checks those.

#### Repeated 401: compare against the original key file

Do not keep rotating credentials without new evidence. To remove Codespaces
environment-variable loading from the test, use the original downloaded private
key file and the API key ID created with that file. This optional diagnostic
ignores **all** credential environment variables and performs only the balance GET.

```bash
mkdir -p .kalshi-credentials
chmod 700 .kalshi-credentials
```

Using the Codespaces file explorer, upload the original file into that ignored
folder as `original.key` (do not commit or paste its contents), then run:

```bash
chmod 600 .kalshi-credentials/original.key
python kalshi_shard_admin.py auth-check --key-file .kalshi-credentials/original.key
```

The paired production API key ID is requested through a hidden terminal prompt,
not a command-line argument or shell history. Do not use redirected input or a
notebook: the tool refuses a prompt that cannot hide input. It does not store the
prompted ID, modify secrets, or allow `--key-file` with a transfer/allocation.

- File check passes but environment check fails: investigate differences in
  credential material/loading; compare the paired ID/file before concluding the
  secrets are wrong. Account/server state can also change between requests.
- Both checks fail: inspect `server_reasons`. A specific rejection guides the
  investigation; do not automatically blame formatting or generate another key.
- Neither result supplies a specific reason: send Kalshi support the sanitized
  diagnostic version, UTC time, host, endpoint, status and request identifier.
  Ask them to identify the authentication denial for that request. Never send
  private keys or signed headers. Access restrictions must be resolved with
  Kalshi; this utility will not change hosts, locations, proxies or IPs to evade them.

## 3. One-time transfer of all available shard-0 cash

First pause account trading workers and their watchdogs **yourself**, with a
graceful handoff only when safe. A maintenance flag can stop new workflow dispatch
without stopping an already-running process. Do not force-cancel a worker that is
managing real exposure. The tool additionally refuses writes when it observes
resting orders, open/unknown positions, or conflicting pending/unknown transfers.

Only after the workers really are paused:

```bash
python kalshi_shard_admin.py transfer-all --execute --workers-paused
```

The tool displays the exact destination and freshly read amount. Type the exact
confirmation phrase it prints, for example (illustrative balance only):

```text
TRANSFER 100.0000 USD FROM SHARD 0 TO SHARD 2
```

It rechecks balances, routing, orders, positions, pending transfers, key scope and
allocation after confirmation. A changed balance aborts instead of silently
transferring a different amount. An allocation other than disabled or 100% to
the destination blocks the transfer because it could automatically undo it.

Requests use exact Decimal arithmetic: **$1 = 10,000 centicents**. An available
balance of $120.4724 would become integer `1204724`, not cents and not rounded
binary float. Both exchange types are `event_contract`; both subaccounts are 0.
No margin, external withdrawal or non-primary subaccount transfer is supported.
[Kalshi transfer request schema](https://docs.kalshi.com/api-reference/portfolio/intra-account-transfer).

## 4. Verify asynchronous completion and preserve the journal

A 200 POST response is only acceptance. The tool polls the returned transfer ID,
verifies source/destination/amount/time, and waits for official `status=complete`.
It then reads both balances and checks them against the saved plan. A mismatch
is reported for manual review, never used as a reason to resend the transfer.
For example, a separate pending incoming credit may settle after confirmation:
that can increase a balance beyond the saved plan, but it does not authorize
another transfer. The one-time amount remains exactly what you confirmed.

```bash
python kalshi_shard_admin.py resume-transfer
python kalshi_shard_admin.py status
```

If the POST response is lost, **do not resubmit**, delete the journal, use a new
Codespace or change working directory to bypass the guard. Find the corresponding
transfer ID in Kalshi's history/support, then verify it against the saved intent:

```bash
python kalshi_shard_admin.py resume-transfer --transfer-id YOUR_CONFIRMED_TRANSFER_ID
```

The private, gitignored `.kalshi-shard-admin/` directory contains `transfer.json`,
`allocation.json` when applicable, and a process-lock file. Intent is atomically
written and fsynced **before** any POST. Later states include `ACCEPTED`, `PENDING`,
`COMPLETED` and `SUBMISSION_UNKNOWN`. Never delete this directory to retry an
uncertain operation. Preserve it privately before deleting/rebuilding a Codespace;
do not upload it as a public artifact. Repeated commands resume the same operation
instead of initiating another transfer. This is deliberately a one-time recovery
tool, not an unattended recurring sweeper.

The local process lock cannot serialize separate machines/Codespaces. Kalshi's
documented transfer request has no client idempotency field, so exactly-once
execution across lost responses/multiple machines cannot be guaranteed. An unknown
response is therefore fail-closed and requires read-only reconciliation.

## 5. Optional: separately enable 100% recurring allocation

This is **not** part of the one-time transfer. It authorizes Kalshi to continually
rebalance sweepable cash across the account, including future funds, to the chosen
market's exchange. Keep workers paused, verify any preceding transfer is complete,
and review all consequences before running:

```bash
python kalshi_shard_admin.py allocation-all
python kalshi_shard_admin.py allocation-all --execute --workers-paused
```

The second command requires a different typed confirmation:

```text
ALLOCATE 100 PERCENT OF SWEEPABLE ACCOUNT CASH TO SHARD 2
```

The actual destination comes from market metadata. The tool verifies the result
with a subsequent GET. Re-running after an uncertain response checks the desired
allocation without blindly POSTing again. No 90/10 policy is silently selected.
If you enable allocation first, Kalshi may sweep the cash before a one-time
transfer is needed; check balances rather than trying to transfer it a second time.
[Kalshi allocation API](https://docs.kalshi.com/api-reference/portfolio/set-target-balance-allocation).

## 6. Trading remains a separate operator decision

The existing `maker_entry_submission_unknown` breaker is intentionally not cleared
by this tool. Sufficient cash and valid credentials do not prove an uncertain order
was never accepted. Reconcile exchange order history, fills, positions and the
durable strategy record before any operator-authorized recovery. Do not replace
the strategy state, erase its ledger, or use a live test order as an automatic
half-open probe. No strategy parameter, workflow live flag or live/shadow checkpoint
is modified by this utility.

HTTP 429, timeout and 5xx responses to an order POST should not be assumed to mean
the exchange rejected it. A retry can duplicate exposure unless the original order
is reconciled first. Transfer success also cannot guarantee later order acceptance,
fill probability, stop price or profitability. Respect Kalshi's account, location,
and API restrictions; the utility does not bypass them.

Exit code 0 means the requested diagnostic/completion check succeeded, not that
live trading is enabled. Exit code 2 means blocked, pending, uncertain or otherwise
requiring review. Secrets and raw API error bodies are never printed.
