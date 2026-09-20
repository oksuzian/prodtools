# prodtools MCP servers

Ask an AI assistant about Mu2e production state in plain language —
"how is MDC2025au doing?", "what datasets came out of it?" — instead of
remembering which CLI to run.

## Quick start

### 1. Connect

Install your own, on a Fermilab node (mu2egpvm, with CVMFS). It takes
about two minutes:

```bash
cd /exp/mu2e/app/users/$USER
git clone https://github.com/Mu2e/prodtools
cd prodtools
bash mcp/scripts/install.sh            # once, ~1 minute
bash mcp/scripts/start_mcp.sh --check  # should print OK three times
```

Every path below assumes that location,
`/exp/mu2e/app/users/<user>/prodtools`, with `<user>` your login. Any
other place works as long as you write the full path: not `~`, and not a
relative one, because the client starts the script from its own
directory. Inside a shell `$USER` does the substitution for you; in a
JSON file or a tool call you type the login out.

Your client then starts it on demand — `.mcp.json` in the clone already
does this for Claude Code; for another client:

```json
{
  "mcpServers": {
    "prodtools": { "command": "/exp/mu2e/app/users/<user>/prodtools/mcp/scripts/start_mcp.sh" }
  }
}
```

Your own server runs as you, so it reads what your credentials can read.

### 2. Ask it things

| Question | Tool it uses |
| --- | --- |
| "What is running right now?" | `list_campaigns(state="active")` |
| "How is MDC2025au doing?" | `campaign_status(campaign="MDC2025au")` |
| "What CeEndpoint datasets exist for MDC2025?" | `find_datasets(pattern="%.mu2e.CeEndpoint%.MDC2025a%", require_files=true)` |
| "How big is dts.mu2e.CeEndpoint.MDC2025ac.art?" | `dataset_details(dataset="dts.mu2e.CeEndpoint.MDC2025ac.art")` |
| "Where are the files of sim.mu2e.MuminusStopsCat.MDC2025ac.art?" | `dataset_files(dataset="sim.mu2e.MuminusStopsCat.MDC2025ac.art", location="tape")` |
| "Does SAM know this file, and where is it?" | `locate_file(name="sim.mu2e.MuminusStopsCat.MDC2025ac.001430_00000000.art")` |
| "What was this file made from?" | `trace_provenance(name="dts.mu2e.CeEndpoint.MDC2025ac.001430_00000000.art", direction="up")` |

Each of these is a real name and answers today. The CeEndpoint dataset
has 2000 files and 5.4 million events; the stopped-muon sample is one
7 GB file on tape; the CeEndpoint file traces back to it, and from there
to the target-stop files it was concatenated from.

Two things to know when you read the answers:

- **Production is the default.** For a campaign you ran yourself, add
  "for user <login>" so the tool passes `user="<login>"` — it switches
  both the ledger and the grid queue to that account. Without it you get
  production's, and an empty result looks exactly like "no campaigns".
- **`state: "unknown"` is not zero.** It means the query failed. The
  campaign may well still be running, so never start a recovery on one.

### 3. Nothing it can break

Every tool here is read-only: no job submission, no SAM definition
create or delete, no ledger change. Submitting is a separate server
(`prodtools-write`) that is not reachable over HTTP at all.

### 4. Submit your own jobs

**[SUBMIT.md](SUBMIT.md)** walks through it: a small CeEndpoint sample
on the newest MDC2025 release with nothing to edit, a run that stays out
of SAM entirely, a G4beamline job, and the traps. The install from
"1. Connect" already set up the second server it uses,
`prodtools-write`. The short version:

    push_cnf(json="/exp/mu2e/app/users/<user>/prodtools/data/examples/ceendpoint.json",
             desc="CeEndpoint", dsconf="MDC2025ax", slice_size=3, run_as="self")
    run_submissions(run_as="self", campaign_id=<id>)
    campaign_status(campaign_id=<id>, mine=true)     # until the queue is empty
    run_submissions(run_as="self")                   # verify and recover

With `run_as="self"` everything lands under your own name and cannot
touch production.

---

Two servers live under `mcp/`, registered in `.mcp.json` at the repo
root and enabled in `.claude/settings.json`.

## `prodtools` (read-only)

Exposes campaign status and dataset discovery as typed tools:
`campaign_status`, `list_campaigns`, `find_datasets`, `dataset_details`,
`locate_file`, `dataset_files`, `trace_provenance`, `run_status`,
`get_server_info`.
It performs **NO writes** — it cannot submit jobs, create or delete SAM
definitions, or modify the submission ledger. That guarantee is why its
tools can be called without deliberation; do not weaken it.

Setup: `bash mcp/scripts/install.sh`.
Health check: `bash mcp/scripts/start_mcp.sh --check`.

### Serve it to other people

    bash mcp/scripts/start_mcp.sh --transport streamable-http \
        --host 0.0.0.0 --port 8008 [--allowed-host <fqdn>:8008]

`--host` defaults to `127.0.0.1`, so nothing reaches the network until
you say so, and `--allowed-host` (repeatable) turns on the SDK's
DNS-rebinding check — left out, that check is off, as on the other
central Mu2e servers. `mcp/deploy/prodtools-mcp.service` is a
`systemd --user` unit for a permanent instance; read its header first,
because a shared server queries SAM and HTCondor as ITS OWN account. The
HTCondor pool wants a bearer token, which lasts about three hours, and
how the hosting account gets and renews one is left open there. A
missing or expired token shows up as `state: "unknown"`, never as zero.

Only the read-only server is servable this way. `prodtools-write` stays
stdio: it submits as mu2epro behind `ksu`, `confirm=true` and a
PreToolUse hook, none of which survives being reached over a port.

### A shared server: planned, not running

Nobody runs a shared instance today; everyone installs their own. The
plan is one on the collaboration's MCP host. When it exists, connecting
will need no install:

    claude mcp add --transport http prodtools http://<host>:8008/mcp

or, for other MCP clients,
`{"mcpServers": {"prodtools": {"type": "http", "url": "http://<host>:8008/mcp"}}}`.
It will answer questions only. Submitting always needs your own install,
because a shared server runs as its host account, not as you.

## `prodtools-write`

Exposes submission: `push_cnf`, `run_submissions`, `submit_once`, and
publishing: `push_file`.

A production campaign takes two calls: `push_cnf(..., slice_size=N)`
builds the cnf, registers it in SAM and creates the campaign, returning
a `campaign_id` for `run_submissions`. That call mirrors `json2jobdef
--prod --enqueue`, which is now the only way json2jobdef runs under
`--prod`.

`push_cnf` identifies the campaign it created by desc+dsconf against a
snapshot of the ledger taken before the CLI ran. If nothing new appears
it RAISES rather than returning a pre-existing campaign — handing back
the wrong id would point `run_submissions` at an unrelated production
campaign.

`push_file(path, location, parents, run_as, confirm=False)` publishes one
already-built file to SAM with its parents through the same `pushOutput`
call a grid job makes (`bin/push_file`). The basename is the SAM name: a
six-field Mu2e file name owned by the identity (`mu2e` for mu2epro).
`location` is tape/disk/scratch. A name already in SAM is refused.

`push_cnf(..., prodtools_dir=...)` forwards `--prodtools-dir` so a
checkout can be run before its release lands on cvmfs; it is refused
for `run_as="mu2epro"` — stricter than json2jobdef's own rule, since
this write surface is the wrong place to accept a production prodtools
override at all.

Every tool takes a required `run_as`:

- `run_as="self"` needs no privilege and writes only your own scratch,
  datasets and ledger (`/exp/mu2e/data/users/$USER/prodtools/`). No
  confirmation and no prompt.
- `run_as="mu2epro"` registers artifacts in production SAM and submits
  production grid jobs. It is refused in-tool unless `confirm=true`
  (`runner.require_confirmed`), and a `PreToolUse` hook
  (`.claude/hooks/mcp-write-guard.sh`, matcher
  `mcp__prodtools-write__.*`) additionally prompts for confirmation.
  Both gates are independent and deliberate: the hook covers the whole
  tool namespace so a future write tool cannot silently escape it, and
  the in-tool refusal survives a hook left un-armed by a settings
  reload.

`confirm=true` is a **model-facing** gate — the model supplies it to
itself — so the hook is the only *human*-in-the-loop checkpoint on a
`run_as="mu2epro"` call. It is written to fail CLOSED: only a
positively parsed `run_as=="self"` passes silently; `run_as=="mu2epro"`,
a missing `run_as`, malformed hook input, an unrecognised value, or a
failing/missing `jq` binary all produce a prompt.

**A settings-hooks edit is not live in an already-running session.**
Registering a new `PreToolUse` matcher in `.claude/settings.json` (as
this one is) requires a `/hooks` reload — a session started before the
edit will call `prodtools-write` tools with the hook un-armed even
though `CLAUDE.md` documents the gate as present. Run `/hooks` (or
start a fresh session) after any change here before relying on the
prompt.

Health check: `bash mcp/scripts/start_write_mcp.sh --check`.

Both launchers share environment setup via `mcp/scripts/_mcp_env.sh`.

## `submissions status` and `--mine`

The `submissions status` verb (see `utils/submissions.py`) reads the
**production ledger by default** — the same ledger the direct-submission
cron uses — *only when the `MU2E_SUBMISSION_DB` env var is unset*; if
it is set, that path wins over the production default (see
`resolve_db`/`build_parser` in `utils/submissions.py`). Pass `--mine` to
read your own ledger
(`/exp/mu2e/data/users/$USER/prodtools/submissions.db`) instead, e.g.
after a `run_as="self"` campaign run through `prodtools-write`. Plain
`submissions status` will not show a self-run campaign; `submissions
status --mine` will.

The MCP status tools take the same idea as two parameters.
`campaign_status` and `list_campaigns` accept `user` and `mine`, and with
neither they read production's ledger and mu2epro's queue, exactly as
before.

`user="<login>"` reads `/exp/mu2e/data/users/<login>/prodtools/
submissions.db` and counts that account's grid queue. Personal ledgers
are world-readable, so this needs no privilege and works under either
transport.

`mine=true` is the same thing derived from the process account. It is
meaningful only over stdio, where the process IS you. A server started
with `--transport streamable-http` refuses it with an
`invalid_argument` naming `user` instead, because there the process
account is the host: a bare `mine` would hand every reader the host's
ledger, and an empty answer from the wrong ledger is indistinguishable
from "no campaigns".

Both axes move together by construction — a call cannot read one
account's ledger against another's queue. Every reply names what it read:
`db_path` at the top level, and `owner` inside each `queue` block.

A ledger somewhere other than `/exp/mu2e/data/users/<login>/prodtools/`
is still not reachable through MCP; `user` is validated as a UNIX login,
not a path. Use the CLI for those:

    bash bin/submissions --db /exp/mu2e/data/users/<them>/prodtools/submissions.db status

## Troubleshooting

**`Error checking if token is valid`** in an input check. Your kerberos
ticket is missing or expired: `klist`, then `kinit`. If the ticket is
fine, your MCP client started the server without it: some clients start
a server with a stripped environment (the MCP Python SDK passes only
`HOME`, `LOGNAME`, `PATH`, `SHELL`, `TERM`, `USER`), which drops
`KRB5CCNAME`. Pass the environment through — `"env": {"KRB5CCNAME":
"..."}` in the client config, or `env=dict(os.environ)` from the SDK.
Claude Code passes its own, so it is not affected.
