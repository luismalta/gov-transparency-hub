from pathlib import Path

import yaml
from dagster._utils import run_with_concurrent_update_guard
from dagster_dbt import DbtCliResource, DbtProject
from dagster_dbt.dbt_project import DagsterDbtProjectPreparer

# Files whose change alters the parsed manifest.
_PROJECT_FILE_PATTERNS = ("*.sql", "*.yml", "*.yaml", "*.csv", "*.md")
_PROJECT_SUBDIRS = ("models", "macros", "seeds", "snapshots", "tests", "analyses")


def _mtime(path: Path) -> float:
    return path.stat().st_mtime if path.exists() else 0.0


def _packages_out_of_date(project: DbtProject) -> bool:
    project_dir = Path(project.project_dir)
    lock = project_dir / "package-lock.yml"
    if not (project_dir / "packages.yml").exists():
        return False
    # No packages.yml vs lock mtime check: git checkout writes both in arbitrary
    # order. Editing packages.yml without `dbt deps` fails loudly in dbt parse.
    if not lock.exists():
        return True
    locked = yaml.safe_load(lock.read_text()).get("packages") or []
    installed = project_dir / "dbt_packages"
    return any(not (installed / package["name"]).is_dir() for package in locked)


def _manifest_out_of_date(project: DbtProject) -> bool:
    project_dir = Path(project.project_dir)
    manifest = Path(project.manifest_path)
    if not manifest.exists():
        return True
    sources = [
        project_dir / name
        for name in ("dbt_project.yml", "profiles.yml", "packages.yml", "package-lock.yml")
    ]
    for subdir in _PROJECT_SUBDIRS:
        for pattern in _PROJECT_FILE_PATTERNS:
            sources.extend((project_dir / subdir).rglob(pattern))
    return max(map(_mtime, sources), default=0.0) > _mtime(manifest)


class StaleOnlyDbtProjectPreparer(DagsterDbtProjectPreparer):
    """Runs dbt deps/parse only when their outputs are out of date.

    Under `dagster dev` every run worker inherits the dev flag and imports this
    module, so the default preparer ran `dbt deps` + `dbt parse` in each of
    them. Concurrent backfill runs then timed out on the shared file locks, or
    parsed while another process was reinstalling dbt_packages.
    """

    def prepare(self, project: DbtProject) -> None:
        project_dir = Path(project.project_dir)
        if _packages_out_of_date(project):
            # Re-checked inside the lock: another process may have just done it.
            run_with_concurrent_update_guard(
                project_dir / "package-lock.yml",
                lambda project: _packages_out_of_date(project)
                and self._prepare_packages(project),
                project=project,
            )
        if _manifest_out_of_date(project):
            run_with_concurrent_update_guard(
                Path(project.manifest_path),
                lambda project: _manifest_out_of_date(project)
                and self._prepare_manifest(project),
                project=project,
            )


# Points to the dbt project path
dbt_project_directory = Path(__file__).absolute().parent.parent / "dbt_pipelines"

dbt_project = DbtProject(project_dir=dbt_project_directory)

# References the dbt project object
dbt_resource = DbtCliResource(project_dir=dbt_project)

# Compiles the dbt project & allow Dagster to build an asset graph
StaleOnlyDbtProjectPreparer().prepare_if_dev(dbt_project)
