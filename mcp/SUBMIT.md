# Submit your own grid jobs

How to go from an installed prodtools to a dataset of your own, by asking
an AI assistant. Setup is in [README.md](README.md) ("1. Connect");
that same `install.sh` sets up the second server used here,
`prodtools-write`, and the same `.mcp.json` registers it.

With `run_as="self"` everything lands under your own name: your scratch
area, files called `*.<user>.*` in SAM, your own ledger at
`/exp/mu2e/data/users/<user>/prodtools/submissions.db`. It cannot touch
production.

Paths below assume your clone is at
`/exp/mu2e/app/users/<user>/prodtools`, with `<user>` your login.

## Before the first one

- `getToken` works in your shell, and `klist` shows a valid kerberos
  ticket;
- `/exp/mu2e/data/users/<user>/` exists and you can write to it (the
  `prodtools/` folder inside is created for you, its parent is not);
- you are a registered Mu2e SAM user — declaring a file fails otherwise.

## A first job: a small CeEndpoint sample

Nothing to edit. `data/examples/ceendpoint.json` makes a small
conversion-electron sample (`CeEndpoint` primaries, resampled from the
stopped-muon sample) with the newest MDC2025 SimJob release: three jobs
of 500 generated events, ten to fifteen minutes on the grid, of which a
bit over half pass the primary filter (848 of 1500 in the reference
run).

Ask the assistant to push that entry and submit it as yourself. It makes
these calls:

    push_cnf(json="/exp/mu2e/app/users/<user>/prodtools/data/examples/ceendpoint.json",
             desc="CeEndpoint", dsconf="MDC2025ax", slice_size=3,
             run_as="self")
    run_submissions(run_as="self", campaign_id=<the id push_cnf returned>)

`push_cnf` builds the job package, registers it in SAM and creates the
campaign; `run_submissions` sends the jobs to the grid. Watch them with
the read-only server:

    campaign_status(campaign_id=<id>, mine=true)

The queue block goes idle, then running, then empty. **Nothing advances
by itself** — there is no cron. Once the queue is empty, tick once more,
this time with no campaign id:

    run_submissions(run_as="self")

That pass checks every job's output against SAM, closes the rows that
are complete and resubmits the ones that are not. It has to be the bare
form: a campaign whose jobs were all submitted is already `complete`,
and a tick scoped to it is refused because it would do nothing.

The result is the dataset `dts.<user>.CeEndpoint.MDC2025ax.art` on
scratch; `dataset_files(dataset=..., location="scratch")` lists its
files.

### Making it yours

- The entry leaves `owner` out, so it defaults to whoever runs it.
- For a second attempt change `dsconf` but keep its `MDC2025ax` prefix:
  it names the release, and `simjob_setup` has to agree with it.
- To move to a newer release, take the last line of
  `ls /cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/ | grep '^MDC2025'`
  and change both.
- For more statistics raise `njobs` rather than `events`: jobs run in
  parallel, and a long job is a job more likely to be lost.
- Other kinds of art job (digitization, mixing, reconstruction, ntuples)
  go the same way with a different entry; `EXAMPLES.md` at the repo root
  has the entry formats, and `data/mdc2025/` has real ones to copy.

A production-sized request is a different thing: the entry `CeEndpoint`
/ `MDC2025ax` in `data/mdc2025/primary_muon.json` (2000 jobs of 5000
events, outputs to persistent disk, owned by `mu2e`). That one is
submitted by the production team with `run_as="mu2epro"`, which needs
`ksu` rights, `confirm=true` and a confirmation prompt (see
`prodtools-write` in [README.md](README.md)). Ask them rather than
running it yourself.

## Try something without touching SAM

For a test or a study whose output nobody else should find, send the
outputs to *outstage*: a directory on scratch, nothing declared. Copy an
entry, change its `outloc` to `{"*.art": "outstage"}`, and submit it
once:

    submit_once(json="/exp/mu2e/app/users/<user>/my_ceendpoint.json",
                desc="CeEndpoint", dsconf="MDC2025ax_try1", run_as="self")
    run_status(name="cnf.<user>.CeEndpoint.MDC2025ax_try1.0", user="<user>")

`submit_once` builds the job package locally, sends every job in one go
and returns a receipt: the run's `name`, its `jobid`, and `outstage`.
`run_status` says `running`, then `done` — or `short`, with the failed
job numbers and their exit codes — and lists each finished job's files
as `<outstage>/<cluster>/<job>/<file>`. A job can only exit 0 after its
copy landed, so exit codes are the whole answer; `unknown` means the
grid could not be asked, never that something failed.

What you give up, all of it on purpose: no recovery of failed jobs (make
a new run under a new `dsconf`), nothing findable through SAM, no
campaign and no ticks, at most 10000 jobs, never as production. The
files are on scratch and the grid forgets exit codes after about two
weeks, so copy what you want to keep. This is a way to look at
something, not a lighter way to make a dataset.

## A G4beamline job

`data/g4bl/g4bl.json` is a three-job G4beamline smoke test, under ten
minutes end to end. It needs no input data and no Musing, but it does
need a clone of the beamline decks and two edits:

```bash
cd /exp/mu2e/app/users/$USER
git clone https://github.com/Mu2e/G4BeamlineScripts
cp prodtools/data/g4bl/g4bl.json my_g4bl.json
# edit my_g4bl.json:
#   "g4bl_dir" -> /exp/mu2e/app/users/<user>/G4BeamlineScripts
#   "dsconf"   -> a name you have never used, e.g. MyTest001
```

Then the same four calls as for the art job:

    push_cnf(json="/exp/mu2e/app/users/<user>/my_g4bl.json", desc="G4blSmoke",
             dsconf="MyTest001", slice_size=3, run_as="self")
    run_submissions(run_as="self", campaign_id=<id>)
    campaign_status(campaign_id=<id>, mine=true)     # until the queue is empty
    run_submissions(run_as="self")                   # verify and recover

The files are then in `nts.<user>.G4blSmoke.MyTest001.root`.

## Things that bite

- **`json` must be an absolute path.**
- **A `desc` + `dsconf` pair is used once.** It names the job package in
  SAM, and a SAM name is never reused — not even after a failed attempt.
  Pick a new `dsconf`. The same holds for `submit_once`, per user.
- **The jobs run the CVMFS release, not your clone.** Whatever
  `readlink /cvmfs/mu2e.opensciencegrid.org/bin/prodtools/current` names
  is the worker code, and it has to be **v3.3.4 or newer**: v3.3.3 and
  older fail on the grid under Python 3.12 (at `import samweb_client`
  up to v3.3.2, and at `import gfal2` for any job with input files up to
  v3.3.3). To run worker code you changed yourself, add
  `prodtools_dir="/exp/mu2e/app/users/<user>/prodtools"` to `push_cnf`
  or `submit_once`: it ships that checkout's `bin/` and `utils/` with the
  jobs. Self only.
- **A failed campaign keeps coming back.** The verify-and-recover pass
  covers every open row in your ledger, not only the campaign you name,
  so jobs that failed yesterday are resubmitted next to today's new
  campaign — with the entry they were created with, which fails the
  same way. Close a campaign you have given up on, from a shell with
  the prodtools environment:
  `submissions --mine cancel <id> --close-rows --note "why"`.
- **Never interrupt `run_submissions`**, and never wrap the CLI it runs
  (`submissions run`) in `timeout`. A kill between "the grid accepted
  the jobs" and "the ledger wrote them down" leaves jobs nothing tracks,
  and the next tick submits the same work again.
- **`Error checking if token is valid`** in an input check means your
  kerberos ticket is missing or expired (`klist`, then `kinit`), or your
  MCP client started the server without `KRB5CCNAME`; see
  "Troubleshooting" in [README.md](README.md).
