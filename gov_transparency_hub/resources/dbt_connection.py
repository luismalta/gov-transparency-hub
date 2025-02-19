from dagster_dbt import DbtCliResource, DbtProject
from pathlib import Path


# Points to the dbt project path
dbt_project_directory = Path(__file__).absolute().parent.parent / "dbt_pipelines"

dbt_project = DbtProject(project_dir=dbt_project_directory)

# References the dbt project object
dbt_resource = DbtCliResource(project_dir=dbt_project)

# Compiles the dbt project & allow Dagster to build an asset graph
dbt_project.prepare_if_dev()
