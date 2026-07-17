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
