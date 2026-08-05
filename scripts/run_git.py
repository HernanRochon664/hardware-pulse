"""Auto-commit and push the tracked runtime database after a successful pipeline run.

Stages only ``data/hardware_pulse.db`` so unrelated local changes are never
committed automatically. Pushes only when there is a change to commit.

Emits a human-readable status message on stdout/stderr so the caller can log
whether the change was pushed, nothing to commit, or an error occurred.

Return codes:
    0  - nothing to commit, or commit+push succeeded
    1  - git push failed (commit error also exits non-zero)
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DB = "data/hardware_pulse.db"


def git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=REPO,
        check=False,
        text=True,
        capture_output=True,
    )


def main() -> int:
    if git("diff", "--quiet", "--cached").returncode != 0:
        git("reset", "-q")

    git("add", DB)

    if git("diff", "--cached", "--quiet").returncode == 0:
        print("DB unchanged — nothing to commit or push")
        sys.exit(0)

    commit = git("commit", "-m", "Update DB")
    if commit.returncode != 0:
        print("Commit failed — changes NOT committed or pushed", file=sys.stderr)
        print(commit.stderr, file=sys.stderr)
        sys.exit(1)

    push = git("push")
    if push.returncode != 0:
        print("Push failed — commit created but NOT pushed", file=sys.stderr)
        print(push.stderr, file=sys.stderr)
        sys.exit(1)

    print("Pushed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())