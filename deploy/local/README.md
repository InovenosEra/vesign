# deploy/local/

Version-controlled canonical copies of scripts that run outside this repo's
normal deploy path (local launchd jobs on the dev Mac, standalone server-side
cron jobs). Each one has a "deployed" counterpart living elsewhere that is
NOT this repo checkout — copy changes over manually after review.

| File | Canonical copy (here) | Deployed copy (runs from here) |
|---|---|---|
| `vesign-daily-sync.sh` | this repo, `feat/ui-redesign` | `~/bin/vesign-daily-sync.sh` on the dev Mac, invoked by launchd (`com.vesign.daily.plist` / dbsync) |
| `vesign-redesign-overlay.py` | this repo, `feat/ui-redesign` | `~/bin/vesign-redesign-overlay.py` on the dev Mac, invoked at the end of `vesign-daily-sync.sh` |
| `vesign-attest.py` | this repo, `feat/ui-redesign` | `/root/vesign-attest/vesign-attest.py` on the production droplet, invoked by root's crontab |

Editing a file here does **not** change live behavior. To deploy a change:
copy the file to its deployed path listed above, keeping ownership/executable
bits intact, and update any dependent cron/launchd entry if the interface
changed.

`vesign-attest.py`'s deployed location is deliberately **outside** `/opt/vesign`
(the app's own git checkout, which tracks `main`) — this repo stays on
`feat/ui-redesign` until launch, so nothing under `/opt/vesign` can come from
this branch yet. See the script's own docstring and the "attestation" section
of project memory for the daily-signal-attestation mechanism this feeds.

## Signal attestation — full setup summary

`vesign-attest.py` runs daily via root's crontab on the production droplet
(`15 7 * * 1-6`, ~15 minutes after the 07:00 signal pipeline), reading
`/opt/vesign/vesign.db` strictly read-only and writing private canonical
JSON snapshots — user-facing fields only (`date`, `ticker`, `direction`,
`tier`, `health`, `ml_5d`, `predicted_upside`; no VQS, no raw model
features) — to `/root/vesign-attest/proof-archive/YYYY/YYYY-MM-DD.json`,
which is never committed anywhere. It pushes only that day's SHA-256 to the
public [vesign-proof](https://github.com/InovenosEra/vesign-proof) repo
(`YYYY/YYYY-MM-DD.sha256`, working copy at `/root/vesign-proof`),
authenticating via a repo-scoped deploy key (`/root/.ssh/vesign_proof_deploy`,
SSH host alias `github-vesign-proof`) that has no access to this private
repo. A one-time genesis attestation (`genesis-through-2026-07-16.sha256`)
covers all 162,603 historical BUY/SELL signals back to 2018-01-02. To verify
any published day once snapshots are released at launch: run
`sha256sum YYYY-MM-DD.json` on the published snapshot and compare the
result to the matching `.sha256` file in `vesign-proof` — a match proves
that day's signals existed no later than that commit's timestamp.
