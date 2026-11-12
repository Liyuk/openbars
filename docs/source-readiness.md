# Source readiness declarations

`SourceCapability.latency` is the adapter's stated minimum delay after session close.
`latency_verified` distinguishes a code-verified bound from an unverified estimate.
A request cutoff is separate from after-close availability and does not establish it.
An unverified source is best effort: do not promise same-day (T+0) availability from it.
The planner retains its calendar/session rules and must not interpret an unverified
zero latency as a readiness guarantee.

| Source and capability | Readiness / latency | Evidence status |
| --- | --- | --- |
| Massive US daily, grouped | 8 hours after 16:00 ET (next day 00:00 ET); earlier grouped daily requests raise retryable `data_not_ready` | Code verified |
| Massive US 1h | 8-hour estimate; no adapter readiness check | Unverified; best effort, no T+0 promise |
| Alpaca SIP US daily and 1h | Requests for today end at `now - 16 minutes` | Request cutoff code verified; after-close latency and complete daily/hourly delivery unverified, best effort, no T+0 promise |
| Alpaca IEX US daily and 1h | No 16-minute request cutoff | Cutoff behavior code verified; after-close latency and complete daily/hourly delivery unverified, best effort, no T+0 promise |
| Yahoo Finance US daily/1h and HK daily | No adapter-enforced availability delay | Unverified; best effort, no T+0 promise |
| Twelve Data US 1h | No adapter-enforced availability delay | Unverified; best effort, no T+0 promise |
| BaoStock CN daily | No adapter-enforced availability delay | Unverified; best effort, no T+0 promise |
| AKShare HK daily | No adapter-enforced availability delay | Unverified; best effort, no T+0 promise |
| Futu OpenD / LongPort HK daily | No adapter-enforced availability delay; no real-account probe recorded | Unverified; best effort, no T+0 promise; planner does not route HK jobs to these sources |
| Binance / Coinbase crypto daily | No adapter-enforced latest completed-period delay | Unverified; best effort, no T+0 promise |

The planner's US daily and hourly windows are governed by its exchange calendar/session
readiness rules. Source-specific verified delays are additionally represented in
capabilities; unverified `latency=0` is not a source promise.
