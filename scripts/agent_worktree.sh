#!/usr/bin/env bash
# Create or repair a second working copy of this repository for another agent
# (or for a long experiment), so that two sessions never write the same files.
#
#   scripts/agent_worktree.sh <path> <branch> [--check]
#
# What it does:
#   - adds a git worktree at <path> on <branch> (the branch is created from
#     main if it does not exist yet);
#   - links the main checkout's untracked data folder into it, because the
#     model code opens panels by paths relative to the repository root;
#   - tells git to ignore that link;
#   - with --check, runs the health check inside the new working copy.
#
# Run it again at any time: it only adds what is missing. Caches and run
# outputs are NOT shared, on purpose, so two agents cannot corrupt each
# other's intermediate files. See llm_fitting/coordination/PROTOCOL.md.

set -euo pipefail

dest="${1:?usage: scripts/agent_worktree.sh <path> <branch> [--check]}"
branch="${2:?usage: scripts/agent_worktree.sh <path> <branch> [--check]}"
check="${3:-}"

here="$(cd "$(dirname "$0")/.." && pwd)"
main="$(git -C "$here" worktree list --porcelain | awk '/^worktree /{print substr($0, 10); exit}')"

if [ ! -e "$dest/.git" ]; then
  if git -C "$main" show-ref --verify --quiet "refs/heads/$branch"; then
    git -C "$main" worktree add "$dest" "$branch"
  else
    git -C "$main" worktree add -b "$branch" "$dest" main
  fi
fi

if [ ! -e "$dest/data" ]; then
  ln -s "$main/data" "$dest/data"
fi

excl="$(git -C "$main" rev-parse --git-common-dir)/info/exclude"
case "$excl" in /*) ;; *) excl="$main/$excl" ;; esac
grep -qxF '/data' "$excl" 2>/dev/null || printf '/data\n' >> "$excl"

echo "working copy: $dest"
echo "branch:       $(git -C "$dest" rev-parse --abbrev-ref HEAD) at $(git -C "$dest" rev-parse --short HEAD)"
echo "data link:    $(readlink "$dest/data")"
echo "git status:   $(git -C "$dest" status --short | wc -l | tr -d ' ') changed or untracked paths"

if [ "$check" = "--check" ]; then
  (cd "$dest" && scripts/health_check.sh)
fi
