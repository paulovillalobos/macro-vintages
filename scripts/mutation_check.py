"""Break each concurrency safeguard on purpose and check that a test notices.

A race test that passes proves little unless it fails when the thing it
guards is removed. For each mutation below, this copies the repository to a
temporary directory, applies one textual change, runs the named tests there
against the test database, and expects them to fail. The working tree is
never modified.

    uv run python scripts/mutation_check.py

Exits 1 if the unmutated tests fail or if any mutation survives.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COPY = ("src", "tests", "migrations", "alembic.ini", "pyproject.toml")


@dataclass(frozen=True)
class Mutation:
    name: str
    path: str
    old: str
    new: str
    tests: tuple[str, ...]


MUTATIONS = (
    Mutation(
        "claim without row locks (no FOR UPDATE SKIP LOCKED)",
        "src/macro_vintages/store/jobs.py",
        "    FOR UPDATE SKIP LOCKED\n)",
        "\n)",
        (
            "tests/integration/test_jobs.py::test_claim_takes_due_jobs_only_once",
            "tests/integration/test_races.py::test_parallel_workers_never_double_ingest",
        ),
    ),
    Mutation(
        "write without checking the lease token",
        "src/macro_vintages/store/jobs.py",
        "    if token != job.lease_token:\n",
        "    if False:\n",
        (
            "tests/integration/test_jobs.py::"
            "test_expired_lease_is_reclaimed_and_stale_writer_is_fenced",
        ),
    ),
    Mutation(
        "stamp versions with transaction start, not time after the lock",
        "src/macro_vintages/store/versions.py",
        'await conn.fetchval("SELECT clock_timestamp()")',
        'await conn.fetchval("SELECT now()")',
        (
            "tests/integration/test_versions.py::test_transaction_time_is_read_after_the_series_lock",
        ),
    ),
    Mutation(
        "token bucket read without locking the key row",
        "src/macro_vintages/store/keys.py",
        "    WHERE key_hash = $1 AND revoked_at IS NULL\n    FOR UPDATE\n",
        "    WHERE key_hash = $1 AND revoked_at IS NULL\n",
        ("tests/integration/test_keys.py::test_concurrent_requests_cannot_overspend",),
    ),
)


def _env(tree: Path) -> dict[str, str]:
    return {
        **os.environ,
        # The copy shadows the editable install of the real tree.
        "PYTHONPATH": f"{tree / 'src'}{os.pathsep}{tree}",
        "MV_LOG_LEVEL": "WARNING",
    }


def _run_tests(tree: Path, tests: tuple[str, ...]) -> tuple[bool, str]:
    """(passed, the first FAILED/ERROR line of the summary)."""
    cmd = [sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *tests]
    done = subprocess.run(  # noqa: S603
        cmd, cwd=tree, env=_env(tree), capture_output=True, text=True, check=False
    )
    reason = next(
        (ln for ln in done.stdout.splitlines() if ln.startswith(("FAILED ", "ERROR tests"))), ""
    )
    return done.returncode == 0, reason


def _imports_from(tree: Path) -> bool:
    probe = "import macro_vintages; print(macro_vintages.__file__)"
    done = subprocess.run(  # noqa: S603
        [sys.executable, "-c", probe], cwd=tree, env=_env(tree), capture_output=True, text=True
    )
    return done.stdout.strip().startswith(str(tree))


def main() -> int:
    all_tests = tuple(sorted({t for m in MUTATIONS for t in m.tests}))
    with tempfile.TemporaryDirectory(prefix="mv-mutation-") as tmp:
        base = Path(tmp) / "base"
        for name in COPY:
            src = ROOT / name
            if src.is_dir():
                shutil.copytree(src, base / name, ignore=shutil.ignore_patterns("__pycache__"))
            else:
                shutil.copy2(src, base / name)
        if not _imports_from(base):
            print("the copy does not shadow the installed package; nothing would be mutated")
            return 1
        if not _run_tests(base, all_tests)[0]:
            print("the unmutated tests fail; fix them first")
            return 1
        print(f"baseline: {len(all_tests)} tests pass")
        survived = 0
        for i, m in enumerate(MUTATIONS):
            tree = Path(tmp) / f"m{i}"
            shutil.copytree(base, tree)
            target = tree / m.path
            text = target.read_text()
            if text.count(m.old) != 1:
                print(f"STALE   {m.name}: pattern not found once in {m.path}")
                survived += 1
                continue
            target.write_text(text.replace(m.old, m.new))
            passed, reason = _run_tests(tree, m.tests)
            survived += passed
            print(f"{'SURVIVED' if passed else 'caught  '} {m.name}")
            if reason:
                print(f"         {reason[:150]}")
    return 1 if survived else 0


if __name__ == "__main__":
    raise SystemExit(main())
