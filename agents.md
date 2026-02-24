## Purpose

- Contribution guidelines for AI/droid agents interacting with this repo.
- Keep changes minimal, safe, and consistent with repo coding standards.

## Setup / Requirements

- This repo is a part of a larger monorepo. The root of monorepo is to be set as `export WORKSPACE=../../../../../`
- To Build the code changes, do `cd $WORKSPACE && make`
- Use project Go toolchain and existing dependencies; avoid adding new ones unless required.
- Prefer repo tools (gofmt, go test, project scripts) over custom commands.
- No credentials or secrets in code, logs, or configs.

## Architecture

- Repository contains code for 2 main microservices deployed in clusters: indexer and projector
- Indexer code is responsible for index and it's lifecycle management on local node. It uses an
  event-loop pattern.
- Projector is responsible for streaming data from local KV "vbuckets", using in-house DCP protocol,
  to indexer nodes, to keep index and data in sync. It uses a gen-server pattern.
- Projector streams data to indexer dataport which takes each mutation and adds them to atomic
  mutatiton queue. Timekeeper, a state machine implementation, then takes a batch of mutations
  from the queue and triggers a flush to underlying storage. Flusher is the concurrent worker
  pool which flushes a mutation to the respective index.
- We also have Metadata Provider (adminport) and Scan Client (scanport) with their respective
  clients. Query process (different microservice), embeds these clients in it's process. Scans
  are performance sensitive code paths. Avoid locks in these code paths. Use atomics if required;
- Metadata provider supplies the index information (indexer node, index metadata, etc) and Scan
  client is used to scatter-gather scanned data from index across indexer nodes. Metadata provider
  is also responsible for coordinating DDL across nodes.
- There is an additional component which is shared across indexer and metadata provider called the
  planner. This is responsible to place indexes across the cluster such that memory, CPU and i/o
  usage is evenly spread across multiple nodes. Planning can happen either during DDLs or rebalance
- Across multiple nodes in the cluster, ns_server aka cbauth (cluster manager microservice) orchestrates
  rebalance, aka topology change. Rebalance Service Manager is responsible for handling all types of
  topology changes.
- Feature flags can be found in `secondary/common/config.go` under `SystemConfig`. Along with this,
  all settings with `*.settings.*` are publicly exposed settings and can be overridden by cluster
  manager service. More about them can be found in the `$WORKSPACE/ns_server` folder.
- Release management is driven in the build pipeline when using make. More information about it
  can be found in CMakeLists.txt. It lists all the dependencies (like shared libraries), golang
  version requirement and how PRODUCT_VERSION is controlled in the package.

## Usage (examples/commands)

- Use `.golangci.yml` file for linter references
- Format Go changes: `secondary/tests/ci/scripts/dolint`
- Validate scope before committing: `git status`, `git diff`
- Run targeted tests touching Go code: `MODE=unit secondary/tests/ci/scripts/dotest` (or narrower packages).
- For a broader change, run full suite of functional tests with `MODE=functional secondary/tests/ci/scripts/dotest`

## Conventions / Policies

- Respect existing patterns and naming; follow surrounding style.
- Keep comments minimal and functional; no README/docs edits unless requested.
- Ensure safety: no unvetted downloads, no external code execution.
