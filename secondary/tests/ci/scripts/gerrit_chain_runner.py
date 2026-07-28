#!/usr/bin/env python3

import json
import os
import subprocess
from collections import defaultdict, deque
from datetime import date, timedelta
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


GERRIT_HOST = os.environ.get("GERRIT_HOST", "review.couchbase.org")
GERRIT_PORT = os.environ.get("GERRIT_PORT", "29418")
GERRIT_USER = os.environ.get("GERRIT_USER", "cbci")
GERRIT_PROJECT = os.environ.get("GERRIT_PROJECT")
LINT_ONLY = os.environ.get("LINT_ONLY", "false").lower() == "true"

SCRIPTS_DIR = Path(__file__).resolve().parent
BUILDER = SCRIPTS_DIR / "builder"
DOLINT = SCRIPTS_DIR / "dolint"

# Projects that dolint knows how to lint and post gerrit feedback for.
LINTABLE_PROJECTS = {"indexing", "gometa"}
GERRIT_LIST_PATH = Path.home() / "gerrit.list"
# Persistent record of (change number, patchset) already handled. A patch
# version is linted at most once — even across restarts of the lint loop —
# so a failing change is not re-linted on every cycle.
LINT_SEEN_PATH = Path.home() / ".lint-seen"
# Marker holding the Monday (ISO date) the seen list was last reset. The list is
# wiped once at the start of each week so every open change is re-linted weekly.
LINT_SEEN_RESET_MARKER = Path.home() / ".lint-seen.reset"


@dataclass
class Change:
    number: int
    project: str
    branch: str
    ref: str
    depends_on: List[int]
    subject: str


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is required")
    return value


def project_path(project: str, workspace: str) -> Optional[Path]:
    mapping = {
        "indexing": Path(workspace) / "goproj/src/github.com/couchbase/indexing",
        "gometa": Path(workspace) / "goproj/src/github.com/couchbase/gometa",
        "plasma": Path(workspace) / "goproj/src/github.com/couchbase/plasma",
        "nitro": Path(workspace) / "goproj/src/github.com/couchbase/nitro",
        "bhive": Path(workspace) / "goproj/src/github.com/couchbase/bhive",
    }
    path = mapping.get(project)
    if path and path.exists():
        return path
    return None


def run(cmd: List[str], cwd: Optional[Path] = None, env: Optional[Dict[str, str]] = None, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=check)


def fetch_changes() -> Dict[int, Change]:
    query_terms = [f"(reviewer:{GERRIT_USER} OR cc:{GERRIT_USER})", "status:open"]
    if GERRIT_PROJECT:
        query_terms.append(f"project:{GERRIT_PROJECT}")
    cmd = [
        "ssh",
        "-p",
        GERRIT_PORT,
        GERRIT_HOST,
        "gerrit",
        "query",
        "--format=JSON",
        "--dependencies",
        "--current-patch-set",
    ] + query_terms

    proc = run(cmd, check=True)
    changes: Dict[int, Change] = {}
    for line in proc.stdout.splitlines():
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "rowCount" in data or "type" in data:
            continue
        number = data.get("number")
        current = data.get("currentPatchSet", {})
        ref = current.get("ref")
        project = data.get("project")
        branch = data.get("branch")
        if not (number and ref and project and branch):
            continue
        depends_on = [int(dep["number"]) for dep in data.get("dependsOn", []) if dep.get("number")]
        changes[int(number)] = Change(
            number=int(number),
            project=project,
            branch=branch,
            ref=ref,
            depends_on=depends_on,
            subject=data.get("subject", ""),
        )
    return changes


def build_chains(changes: Dict[int, Change]) -> List[List[int]]:
    adjacency: Dict[int, Set[int]] = defaultdict(set)
    indegree: Dict[int, int] = defaultdict(int)
    for change in changes.values():
        for dep in change.depends_on:
            if dep in changes:
                adjacency[dep].add(change.number)
                indegree[change.number] += 1

    reverse: Dict[int, Set[int]] = defaultdict(set)
    for src, dests in adjacency.items():
        for dst in dests:
            reverse[dst].add(src)

    chains: List[List[int]] = []
    seen: Set[int] = set()

    def component(start: int) -> Set[int]:
        stack = [start]
        comp: Set[int] = set()
        while stack:
            node = stack.pop()
            if node in comp:
                continue
            comp.add(node)
            stack.extend(adjacency.get(node, ()))
            stack.extend(reverse.get(node, ()))
        return comp

    for num in changes:
        if num in seen:
            continue
        comp_nodes = component(num)
        seen.update(comp_nodes)
        local_indegree = {n: indegree.get(n, 0) for n in comp_nodes}
        queue = deque(sorted(n for n in comp_nodes if local_indegree[n] == 0))
        ordered: List[int] = []
        while queue:
            node = queue.popleft()
            ordered.append(node)
            for neighbor in adjacency.get(node, ()):
                if neighbor in local_indegree:
                    local_indegree[neighbor] -= 1
                    if local_indegree[neighbor] == 0:
                        queue.append(neighbor)
        remaining = [n for n in comp_nodes if n not in ordered]
        ordered.extend(sorted(remaining))
        chains.append(ordered)
    return chains


def ensure_clean(path: Path) -> None:
    status = run(["git", "status", "--porcelain"], cwd=path)
    if status.stdout.strip():
        raise RuntimeError(f"Working tree not clean at {path}")


def capture_state(path: Path) -> Tuple[str, str]:
    branch = run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=path).stdout.strip()
    head = run(["git", "rev-parse", "HEAD"], cwd=path).stdout.strip()
    return branch, head


def restore_state(path: Path, state: Tuple[str, str]) -> None:
    branch, head = state
    if branch not in ("HEAD", "(detached)"):
        run(["git", "checkout", branch], cwd=path, check=False)
    run(["git", "reset", "--hard", head], cwd=path, check=False)
    # No -x: ignored paths hold generated build artifacts baked into the image
    # (notably secondary/protobuf/*/*.pb.go, produced by protoc at build time).
    # Removing them makes every later lint fail with undefined protobuf types,
    # since the lint service never rebuilds.
    run(["git", "clean", "-fd"], cwd=path, check=False)


def force_clean(path: Path) -> None:
    """Discard any working-tree changes and untracked files.

    The container runs `builder` (and other tooling) before this script, which
    can leave the repo dirty. Rather than fail, reset back to HEAD so each chain
    starts from a clean base. The pristine state is captured before this runs and
    restored in process_chain's finally block.

    Ignored paths are left alone (no -x) — see restore_state.
    """
    run(["git", "reset", "--hard"], cwd=path, check=False)
    run(["git", "clean", "-fd"], cwd=path, check=False)


def apply_patch(change: Change, workspace: str) -> None:
    repo_path = project_path(change.project, workspace)
    if not repo_path:
        raise RuntimeError(f"Unknown or missing project path for {change.project}")
    ensure_clean(repo_path)
    remote = f"ssh://{GERRIT_HOST}:{GERRIT_PORT}/{change.project}"
    run(["git", "fetch", remote, change.ref], cwd=repo_path)
    run(["git", "cherry-pick", "FETCH_HEAD"], cwd=repo_path)


def change_key(change: Change) -> Optional[Tuple[int, str]]:
    """Stable identity for one patch version: (change number, patchset)."""
    parts = change.ref.split("/")
    if len(parts) >= 5:
        return (change.number, parts[4])
    return None


def load_seen() -> Set[Tuple[int, str]]:
    """Load (change, patchset) keys already linted in a previous cycle."""
    seen: Set[Tuple[int, str]] = set()
    if not LINT_SEEN_PATH.exists():
        return seen
    for line in LINT_SEEN_PATH.read_text().splitlines():
        num, _, patchset = line.strip().partition(",")
        if num and patchset:
            try:
                seen.add((int(num), patchset))
            except ValueError:
                continue
    return seen


def mark_seen(keys: Set[Tuple[int, str]]) -> None:
    """Append (change, patchset) keys to the persistent seen store."""
    if not keys:
        return
    with open(LINT_SEEN_PATH, "a") as f:
        for num, patchset in sorted(keys):
            f.write(f"{num},{patchset}\n")


def maybe_weekly_reset() -> None:
    """Delete the seen list once at the start of each week (Monday-anchored).

    Runs every cycle but only acts the first time it sees a new week's Monday,
    so the whole open set is re-linted once a week without wiping mid-week.
    """
    this_monday = date.today() - timedelta(days=date.today().weekday())
    last_reset: Optional[date] = None
    if LINT_SEEN_RESET_MARKER.exists():
        try:
            last_reset = date.fromisoformat(LINT_SEEN_RESET_MARKER.read_text().strip())
        except ValueError:
            last_reset = None
    if last_reset is not None and last_reset >= this_monday:
        return
    try:
        LINT_SEEN_PATH.unlink()
        print(f"lint-seen list reset for week of {this_monday.isoformat()}")
    except FileNotFoundError:
        pass
    LINT_SEEN_RESET_MARKER.write_text(this_monday.isoformat())


def write_gerrit_list(chain: List[int], changes: Dict[int, Change], include_keys: Set[Tuple[int, str]]) -> None:
    """Write ~/gerrit.list for the current chain, in the format dolint expects.

    Only lintable projects (indexing, gometa) whose (change, patchset) is in
    include_keys get entries — i.e. patch versions not linted in a prior cycle.
    Ref format is refs/changes/XX/<patch_id>/<patchset>, matching dobuild's awk.
    """
    with open(GERRIT_LIST_PATH, "w") as f:
        for num in chain:
            change = changes.get(num)
            if not change or change.project not in LINTABLE_PROJECTS:
                continue
            if change_key(change) not in include_keys:
                continue
            parts = change.ref.split("/")
            if len(parts) >= 5:
                patch_id, patchset = parts[3], parts[4]
                f.write(f"{change.project},{patch_id},{patchset}\n")


def run_builder_and_dolint(workspace: str, env: Dict[str, str]) -> None:
    env = env.copy()
    if not LINT_ONLY:
        run([str(BUILDER)], cwd=workspace, env=env)
    run([str(DOLINT)], cwd=workspace, env=env)


def process_chain(chain: List[int], changes: Dict[int, Change], workspace: str, env: Dict[str, str], seen: Set[Tuple[int, str]]) -> None:
    chain_desc = ' -> '.join(str(n) for n in chain)

    # Lintable changes in this chain whose patch version has not been linted yet.
    new_keys: Set[Tuple[int, str]] = set()
    for num in chain:
        change = changes.get(num)
        if not change or change.project not in LINTABLE_PROJECTS:
            continue
        key = change_key(change)
        if key is not None and key not in seen:
            new_keys.add(key)

    if not new_keys:
        print(f"Skipping chain {chain_desc}: all lintable patch versions already linted")
        return

    print(f"Processing chain: {chain_desc}")
    states: Dict[Path, Tuple[str, str]] = {}
    touched: List[Path] = []
    try:
        for number in chain:
            change = changes[number]
            repo_path = project_path(change.project, workspace)
            if not repo_path:
                print(f"Skipping {number}: no path for project {change.project}")
                continue
            if repo_path not in states:
                states[repo_path] = capture_state(repo_path)
                force_clean(repo_path)
            apply_patch(change, workspace)
            touched.append(repo_path)
        if touched:
            # Only the new patch versions go in gerrit.list, so dolint lints
            # each patchset exactly once.
            write_gerrit_list(chain, changes, new_keys)
            run_builder_and_dolint(workspace, env)
    finally:
        # Persist first: a failure anywhere above (cherry-pick, build, lint)
        # must still mark these patch versions as handled so they are not
        # retried on the next cycle.
        mark_seen(new_keys)
        seen.update(new_keys)
        for path, state in states.items():
            restore_state(path, state)


def main() -> int:
    workspace = require_env("WORKSPACE")
    if not LINT_ONLY:
        require_env("MANIFEST")
        require_env("RELEASE")

    changes = fetch_changes()
    if not changes:
        print("No matching gerrit changes found with reviewer/cc", GERRIT_USER)
        return 0

    chains = build_chains(changes)
    env = os.environ.copy()
    maybe_weekly_reset()
    seen = load_seen()
    for chain in chains:
        # Isolate failures so one bad chain does not block the rest of the cycle;
        # the failing chain's patch versions are already marked seen inside
        # process_chain, so it will not be retried until a new patchset lands.
        try:
            process_chain(chain, changes, workspace, env, seen)
        except Exception as exc:
            print(f"Chain {' -> '.join(str(n) for n in chain)} failed: {exc}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Failed: {exc}")
        raise
