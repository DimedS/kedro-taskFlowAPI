from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from kedro.framework.session import KedroSession
from kedro.framework.project import configure_project

from airflow.decorators import dag, task

class KedroOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        package_name: str,
        pipeline_name: str,
        node_name: str,
        project_path: str | Path,
        env: str,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.package_name = package_name
        self.pipeline_name = pipeline_name
        self.node_name = node_name
        self.project_path = project_path
        self.env = env

    def execute(self):
        configure_project(self.package_name)
        with KedroSession.create(project_path=self.project_path,
                                 env=self.env) as session:
            session.run(self.pipeline_name, node_names=[self.node_name])


# Kedro settings required to run your pipeline
env = "local"
pipeline_name = "__default__"
project_path = Path.cwd()
package_name = "new_kedro_project"

@dag(
    dag_id="kedro-fastFlow-2",
    start_date=datetime(2023,1,1),
    max_active_runs=3,
    # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    schedule_interval="@once",
    catchup=False,
    # Default settings applied to all tasks
    default_args=dict(
        owner="airflow",
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5)
    )
)
def kedro_project():
    @task
    def preprocess_companies_node(*args):
        KedroOperator(
            task_id="preprocess-companies-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="preprocess_companies_node",
            project_path=project_path,
            env=env,
        ).execute()
    @task
    def preprocess_shuttles_node(*args):
        KedroOperator(
            task_id="preprocess-shuttles-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="preprocess_shuttles_node",
            project_path=project_path,
            env=env,
        ).execute()
    @task
    def create_model_input_table_node(*args):
        KedroOperator(
            task_id="create-model-input-table-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="create_model_input_table_node",
            project_path=project_path,
            env=env,
        ).execute()
    @task
    def split_data_node(*args):
        KedroOperator(
            task_id="split-data-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="split_data_node",
            project_path=project_path,
            env=env,
        ).execute()
    @task
    def train_model_node(*args):
        KedroOperator(
            task_id="train-model-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="train_model_node",
            project_path=project_path,
            env=env,
        ).execute()
    @task
    def evaluate_model_node(*args):
        KedroOperator(
            task_id="evaluate-model-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="evaluate_model_node",
            project_path=project_path,
            env=env,
        ).execute()
    a = preprocess_companies_node()
    b = preprocess_shuttles_node()
    c = create_model_input_table_node(a, b)
    d = split_data_node(c)
    e = train_model_node(d)
    evaluate_model_node(d, e)
kedro_project()