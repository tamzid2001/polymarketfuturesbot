# Operator-run shard-2 order test

This tests authentication, active-market discovery, a real WebSocket quote,
shard-local funding, order acceptance, cancellation and fill reconciliation.
It is **not** the trading strategy or evidence of profitability. It does not
transfer money. If funds were already moved in Kalshi's UI, do not transfer
them again. An HTTP 401 is an authentication failure, not proof of missing
shard funding; the test stops there without submitting.

Run these commands in the repository root in your Codespaces terminal, with
the existing `KALSHI_PROD_API_KEY` (or `KALSHI_API_KEY_ID`) and
`KALSHI_PRIVATE_KEY` secrets loaded. Never paste credentials into commands,
chat, a commit or logs. Use the same requirements/environment as the trader.

```bash
gh pr checkout 87
git pull --ff-only
source .venv/bin/activate
python -m pip install -r requirements_kalshi_settlement_trader.txt
python kalshi_order_smoke_test.py
```

The last command is **read-only**, including the WebSocket subscription. It
discovers the active KXBTC15M ticker through the API and requires its metadata
to confirm shard 2, a binary market and a compatible price grid. No invented
ticker or aggregate-balance fallback. It defaults to testing YES; `--side no`
tests NO instead. An explicit `--ticker` must also pass these validations.

## Send the one test order yourself

First pause all trading workers and the watchdog yourself. An empty order book
for your account at preflight cannot prove another process will not submit a
moment later; `--workers-paused` is your attestation, not a remote worker lock.
The tool refuses a non-flat or resting-order shard-2 account.

```bash
python kalshi_order_smoke_test.py --execute --workers-paused
```

Read the preview and type its exact confirmation phrase. The tool checks:

- Authentication, matching key write scope and unrestricted primary account.
- Exchange active, market open, at least 60 seconds remaining.
- Shard **2** available funds ≥$0.02 (one-cent principal plus fee allowance).
- Fresh two-sided selected-market WebSocket prices, including exchange time.
- A 1¢ buy is at least 10¢ below the selected ask and below its bid.
- No existing test order with the same deterministic client ID.

It submits **one contract at 1¢**, post-only, GTC with a **30-second server
expiration**, then immediately attempts cancellation and verifies order,
fills and position through shard-2 REST reads. That short expiry belongs only
to this smoke test; production strategy GTC lifetime is unchanged. Rechecking
after confirmation avoids using stale quotes, funding or expiration times.

Kalshi V2 uses YES-book prices: YES buy is `bid` at `0.0100`; the economically
equivalent NO buy is `ask` at `0.9900`. Both risk one cent of principal for
one contract, plus any applicable fees. This follows the documented
[V2 order contract](https://docs.kalshi.com/api-reference/orders/create-order-v2).
Cancellation passes the market ticker and shard explicitly, as required by
the [V2 cancellation API](https://docs.kalshi.com/api-reference/orders/cancel-order-v2).

**A deep post-only order can fill.** Neither price distance nor expiry
guarantees zero fills, zero fees or successful cancellation. If any quantity
fills, the tool reports `FILLED_REVIEW_REQUIRED`, actual quantity, fees and
remaining position, then exits nonzero. It does not submit a liquidation order.
Review the real position in Kalshi before resuming the strategy; it is not
automatically added to the strategy's P&L/recovery state.

## Results and recovery

Example success shape (illustrative, not a performed live test):

```text
PREFLIGHT_PASS exchange_index=2 write_scope=PASS
WS_CONNECTED
ORDER_TEST_PREVIEW quantity=1.00 economic_limit=0.01 selected_ask=...
ORDER_ACK_RECEIVED order_id=...
ORDER_RECONCILED status=resting filled_quantity=0.00 remaining_quantity=1.00
ORDER_TEST_RESULT state=CANCELED_NO_FILL position=0 fees=0
```

An acknowledgment is **not a fill**. `CANCELED_NO_FILL` requires a terminal
order with zero remaining, matching fill records and flat position. Acceptance
of one test proves only that request worked then; it cannot guarantee future
orders, strategy signals, stops or account permissions will succeed.

The fsynced journal is `.kalshi-order-smoke-test/order-smoke-test.json`, ignored
by Git, separate from strategy state/checkpoints. It is written **before** the
single POST. Preserve it across restarts; never delete it to retry an uncertain
submission. A repeated invocation with a journal only reconciles the prior
test; it never creates another test. Client IDs are stable per market/side,
and the exchange is checked before creation too. Local locks/journals cannot
guarantee exclusion across multiple machines or deleted histories.

Read-only recovery:

```bash
python kalshi_order_smoke_test.py --reconcile-only
```

Allow cancellation of the already-recorded test only (no new creation):

```bash
python kalshi_order_smoke_test.py --reconcile-only --execute --workers-paused
```

Unknown submission, delayed REST visibility or unconfirmed cancellation exits
nonzero as `UNRESOLVED_DO_NOT_REPEAT`. A later invocation can recheck/cancel
only that test. If the journal is missing, the credential changed, or evidence
does not reconcile, inspect your Kalshi UI/support; do not blindly resubmit.
Server expiry is a backup during a process/network failure, not a substitute
for verifying the actual order and any fills. Keep workers paused until clear.

No production code rollout, workflow activation, breaker reset, new credential,
fund transfer or automatic sell occurs. Tests are mocked and use no account
secrets or live network/order requests.
