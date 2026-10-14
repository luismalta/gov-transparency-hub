import os
import time

import pytest
from dagster_dbt import DbtProject

from gov_transparency_hub.resources.dbt_connection import StaleOnlyDbtProjectPreparer


class RecordingPreparer(StaleOnlyDbtProjectPreparer):
    """Records the dbt commands instead of running them."""

    def __init__(self):
        super().__init__()
        self.calls = []

    def _prepare_packages(self, project):
        self.calls.append("deps")

    def _prepare_manifest(self, project):
        self.calls.append("parse")


def _touch(path, age_seconds=0):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    stamp = time.time() - age_seconds
    os.utime(path, (stamp, stamp))


@pytest.fixture
def project(tmp_path):
    (tmp_path / "dbt_project.yml").write_text("name: demo\nprofile: demo\n")
    (tmp_path / "packages.yml").write_text("packages: []\n")
    (tmp_path / "package-lock.yml").write_text(
        "packages:\n  - name: dbt_utils\n    package: dbt-labs/dbt_utils\n"
    )
    _touch(tmp_path / "models" / "a.sql", age_seconds=100)
    _touch(tmp_path / "dbt_packages" / "dbt_utils" / "dbt_project.yml")
    _touch(tmp_path / "target" / "manifest.json")
    for name in ("dbt_project.yml", "packages.yml", "package-lock.yml"):
        _touch(tmp_path / name, age_seconds=100)
    return DbtProject(project_dir=tmp_path)


def test_up_to_date_project_runs_no_dbt_command(project):
    preparer = RecordingPreparer()

    preparer.prepare(project)

    assert preparer.calls == []


def test_missing_manifest_triggers_parse(project):
    (project.project_dir / "target" / "manifest.json").unlink()
    preparer = RecordingPreparer()

    preparer.prepare(project)

    assert preparer.calls == ["parse"]


def test_model_edited_after_manifest_triggers_parse(project):
    _touch(project.project_dir / "target" / "manifest.json", age_seconds=50)
    _touch(project.project_dir / "models" / "a.sql")
    preparer = RecordingPreparer()

    preparer.prepare(project)

    assert preparer.calls == ["parse"]


def test_missing_locked_package_triggers_deps(project):
    (project.project_dir / "dbt_packages" / "dbt_utils" / "dbt_project.yml").unlink()
    (project.project_dir / "dbt_packages" / "dbt_utils").rmdir()
    preparer = RecordingPreparer()

    preparer.prepare(project)

    assert preparer.calls[0] == "deps"


def test_checkout_order_of_package_files_does_not_trigger_deps(project):
    # git checkout writes packages.yml and package-lock.yml in arbitrary order,
    # microseconds apart; that must not reinstall packages on every import.
    lock = project.project_dir / "package-lock.yml"
    stamp = lock.stat().st_mtime + 0.0001
    os.utime(project.project_dir / "packages.yml", (stamp, stamp))
    preparer = RecordingPreparer()

    preparer.prepare(project)

    assert "deps" not in preparer.calls


def test_missing_lock_triggers_deps(project):
    (project.project_dir / "package-lock.yml").unlink()
    preparer = RecordingPreparer()

    preparer.prepare(project)

    assert preparer.calls[0] == "deps"


def test_deleted_model_triggers_parse(project):
    # Deleting (or moving) a file leaves no newer file behind, only a newer
    # directory; the manifest must still be rebuilt to drop the model.
    _touch(project.project_dir / "target" / "manifest.json", age_seconds=50)
    _touch(project.project_dir / "models" / "b.sql", age_seconds=100)
    os.utime(project.project_dir / "models", (time.time() - 100, time.time() - 100))
    (project.project_dir / "models" / "b.sql").unlink()
    preparer = RecordingPreparer()

    preparer.prepare(project)

    assert preparer.calls == ["parse"]
