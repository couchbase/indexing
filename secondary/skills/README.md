# GSI Skills

Claude skills for GSI (Global Secondary Indexing) engineering work:

| Skill | Purpose |
|---|---|
| **cbcollect-investigation** | Investigate cbcollect_info log bundles for indexer issues — crashes, failovers, upgrades, rebalance problems, slow scans, mutation-queue backlogs, warmup performance — for CBSE/MB cases. |
| **ci-investigation** | Triage GSI CI failures from ci2i-unstable — both build breaks (`gsi-current.html` + make logs) and functional-test failures (`gsi-*.fail.html`). |
| **run-couchbase-server** | Run and drive a local dev cluster from a source checkout — start/stop `cluster_run` with any topology, spawn offset nodes for mixed-mode clusters, add/remove nodes, load data (cbworkloadgen, randdocs), create GSI indexes and verify scans, rebuild + hot-restart the indexer, smoke-test end-to-end. |

All skills share one path convention and one config file, so setup is done once.

---

## 1. Installation

### Claude Code (recommended)

Unpack each `.skill` file (it is a zip) into your personal skills directory:

```bash
mkdir -p ~/.claude/skills
unzip cbcollect-investigation.skill -d ~/.claude/skills/
unzip ci-investigation.skill        -d ~/.claude/skills/
unzip run-couchbase-server.skill    -d ~/.claude/skills/
```

Result:

```
~/.claude/skills/
├── cbcollect-investigation/
│   ├── SKILL.md
│   └── references/          # workflows, gotchas, commands, scan/flush deep dives, build-and-test
├── ci-investigation/
│   ├── SKILL.md
│   └── references/          # known failure patterns (cgo invariant, TMPFAIL cascades, ...)
└── run-couchbase-server/
    ├── SKILL.md             # cluster ops: start/init/topology/offset nodes/add-remove/load/index
    └── smoke.sh             # end-to-end GSI smoke driver (topology-agnostic, run on request)
```

`run-couchbase-server` can also live inside a checkout at
`<top>/.claude/skills/run-couchbase-server/` — Claude Code discovers it
there automatically for anyone working in that tree.

To share with the team instead, check the two folders into a repo (or the
marketplace your org uses) — they contain no machine-specific paths.

### Claude.ai (web/desktop)

Upload the `.skill` files under Settings → Capabilities → Skills. Note the
skills assume local filesystem access to your logs and repos, so Claude Code
is where they are most useful.

---

## 2. One-Time Environment Setup

The skills resolve four variables. Two are **required**, the rest have
**defaults you can override**:

| Variable | Required? | Default | Meaning |
|---|---|---|---|
| `WORKSPACE` | yes | — | Top of the Couchbase source checkout (`repo init` root), e.g. `$HOME/Repos/Totoro/top` |
| `ANALYSIS`  | yes | — | Per-case analysis root, e.g. `$HOME/Analysis` (holds `CBSE-NNNNN/`, `MB-NNNNN/`, `cifail/`) |
| `INDEXING`  | derived | `$WORKSPACE/goproj/src/github.com/couchbase/indexing` | The indexing repo |
| `CIFAIL`    | optional | `$ANALYSIS/cifail` | Where CI-failure artifacts are staged |
| `DOCS`      | optional | `$INDEXING/secondary/docs/llmdocs` | Deep-dive subsystem docs (ship inside the indexing repo). Override to point at a personal notes vault instead. |

`run-couchbase-server` only needs `WORKSPACE` (the checkout to run the
cluster from); it ignores the rest.

Create the shared config file (both skills source it automatically):

```bash
mkdir -p ~/.config/couchbase-gsi
cat > ~/.config/couchbase-gsi/paths.env <<'EOF'
export WORKSPACE=$HOME/Repos/Totoro/top
export ANALYSIS=$HOME/Analysis
export INDEXING=$WORKSPACE/goproj/src/github.com/couchbase/indexing

# Optional overrides — delete these lines to use the defaults:
# export CIFAIL=$ANALYSIS/cifail
# export DOCS=$HOME/Documents/ObsidianVault/Couchbase/Docs/GSI
export DOCS=${DOCS:-$INDEXING/secondary/docs/llmdocs}
EOF
```

A teammate only edits `WORKSPACE` and `ANALYSIS` for their machine.

If the config file is missing, the skills fall back gracefully: they use
already-exported env vars, try to auto-discover the checkout, ask for
anything still unknown, and offer to write this file for you.

---

## 3. Staging Work for the Skills

**cbcollect case** — one folder per case ID under `$ANALYSIS`:

```
$ANALYSIS/CBSE-12345/
├── cbcollect_info_ns_1@node1@.../   # unpacked cbcollect bundles
├── cbcollect_info_ns_1@node2@.../
└── CBSE-12345.md                    # case summary — the skill writes this
```

**CI failure** — one folder per incident under `$CIFAIL`:

```
$CIFAIL/jul15-build-break/
├── gsi-current.html                 # saved from ci2i-unstable — defines the exact CI code state
└── logs-*.tar.txt                   # make output (build breaks)
# or, for test failures:
└── gsi-15.07.2026-08.30.fail.html   # the failing run's report
```

---

## 4. Using the Skills

Just describe the task — the skills trigger on natural phrasing. Examples:

**cbcollect-investigation**

- "Investigate the cbcollect in `~/Analysis/CBSE-12345` — the customer saw an indexer crash during upgrade"
- "Build a failover timeline for MB-67890"
- "Why are GSI scans slow in this bundle? Check primedur and retransmissions"
- "num_items_flushed is stuck — check the mutation queue and the goroutine dump"
- "Measure indexer warmup duration across all nodes"

**ci-investigation**

- "CI is red — artifacts are in `~/Analysis/cifail/jul15`"
- "cbq-engine fails with `fatal error: lz4.h: No such file or directory`, triage it"
- "Triage this fail.html and tell me which Gerrit change broke it"
- "Half of the vector test package is failing — find the root cause"

**run-couchbase-server**

- "Setup a cluster n0:kv+index,n1:n1ql+index"
- "Load 10K docs with cbworkloadgen, create an index on name and check a scan works"
- "Load 5K randdocs docs and index the city field"
- "Start 2 more nodes at offset 2 and add one to the cluster as an index node"
- "Rebuild the indexer and restart it in the running cluster"
- "Run the smoke test"

Both skills are deliberately non-autonomous at the start: they present a plan
and wait for your direction before reading logs or running builds. The
deliverable is always a written analysis in the case folder (plus a patch
file for CI fixes) — never commits into the repos.

---

## 5. How the Skills Use the In-Repo Docs

With the default `DOCS`, the deep-dive docs live inside the indexing repo at
`secondary/docs/llmdocs/`. Because they version with the code, the
cbcollect skill reads them **at the deployed commit** when investigating a
production bundle whose build differs from local HEAD:

```bash
git show <deployed-commit>:secondary/docs/llmdocs/indexer_snapshot_gen.md
```

The deployed commit itself is extracted from the XML manifest embedded in
`couchbase.log` of any cbcollect bundle — the skills' first step in every
code-level analysis.

---

## 6. Updating the Skills

The skills encode hard-won case learnings (`references/gotchas.md`,
`references/failure-patterns.md`). When an investigation teaches you a new
misleading pattern or failure signature, add it there — future sessions (and
teammates) inherit it. Keep `SKILL.md` short and put detail in `references/`.
