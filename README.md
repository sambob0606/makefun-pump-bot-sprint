[![make.fun](https://make.fun/badge/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed.svg)](https://make.fun/competitions/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed)
[![rank](https://make.fun/badge/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed/sambob0606/makefun-pump-bot-sprint/rank.svg)](https://make.fun/competitions/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed)
[![reward](https://make.fun/badge/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed/sambob0606/makefun-pump-bot-sprint/reward.svg)](https://make.fun/competitions/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed)

# Pump Bot Sprint — paper trading bot

A paper trading bot for the [Pump.fun Bot Sprint](https://make.fun/competitions/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed) competition on [make.fun](https://make.fun). It trades paper SOL on a replay of pump.fun bonding-curve data — no real trading, no real SOL, no wallet interaction.

## Strategy

Early-momentum sniper with strict exits, in `main.py` (Python 3, standard library only, speaks `agent-fun/replay/v2` over stdin/stdout):

- **Watch** every new bonding curve from its `CreateEvent`.
- **Buy** when a 30-slot window shows real demand: ≥ 4 buy trades from ≥ 3 distinct wallets, net buy flow ≥ 0.4 SOL, price up 2–60% over the window while sitting within 10% of the window high, and no single wallet dominating the buys.
- **Stake** 0.10 SOL per coin (capped at 1% of the coin's real SOL reserves), pyramiding to at most 0.20 SOL while the signal holds.
- **Exit** everything at +50% (take profit), −25% (stop loss), 900 slots held, or fading flow. One round trip per mint, then never re-enter — the bot rotates through fresh coins toward the 20-coin minimum.
- At most 5 open positions, per the harness caps. Fully deterministic: no clock, no environment reads, no unseeded randomness.

## Files

- `main.py` — the bot (entrypoint, speaks the replay protocol)
- `LICENSE` — MIT

## Grade locally

From the unpacked kit directory:

```sh
node cli/makefun.mjs grade-local --kit . --bot <path-to-this-repo> --driver process
```
