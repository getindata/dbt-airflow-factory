from os import path

from dbt_airflow_factory.airflow_dag_factory import AirflowDagFactory
from dbt_airflow_factory.operator import EphemeralOperator
from tests.utils import task_group_prefix_builder, test_dag


def _get_ephemeral_name(model_name: str) -> str:
    return f"{model_name}__ephemeral"


def test_ephemeral_dag_factory():
    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "ephemeral_operator")

    # when
    dag = factory.create()

    # then
    assert len(dag.tasks) == 16

    task_group_names = [
        el
        for node_name in ["model1", "model4", "model6"]
        for el in [
            task_group_prefix_builder(node_name, "test"),
            task_group_prefix_builder(node_name, "run"),
        ]
    ]
    ephemeral_task_names = [
        node_name + "__ephemeral"
        for node_name in [
            "model2",
            "model3",
            "model5",
            "model7",
            "model8",
            "model9",
            "model10",
            "model11",
        ]
    ]
    assert set(dag.task_ids) == set(["dbt_seed", "end"] + task_group_names + ephemeral_task_names)

    for ephemeral_task_name in ephemeral_task_names:
        assert isinstance(dag.task_dict[ephemeral_task_name], EphemeralOperator)


def test_no_ephemeral_dag_factory():
    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "no_ephemeral_operator")

    # when
    dag = factory.create()

    # then
    assert len(dag.tasks) == 8

    task_group_names = [
        el
        for node_name in ["model1", "model4", "model6"]
        for el in [
            task_group_prefix_builder(node_name, "test"),
            task_group_prefix_builder(node_name, "run"),
        ]
    ]
    assert set(dag.task_ids) == set(["dbt_seed", "end"] + task_group_names)

    for task_name in task_group_names:
        assert not isinstance(dag.task_dict[task_name], EphemeralOperator)


def test_ephemeral_tasks():
    with test_dag():
        factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "ephemeral_operator")
        tasks = factory._builder.parse_manifest_into_tasks(factory._manifest_file_path())

    # then
    assert (
        task_group_prefix_builder("model1", "test")
        in tasks.get_task("model.dbt_test.model1").execution_airflow_task.downstream_task_ids
    )
    assert (
        task_group_prefix_builder("model1", "run")
        in tasks.get_task("model.dbt_test.model1").test_airflow_task.upstream_task_ids
    )

    assert (
        task_group_prefix_builder("model1", "test")
        in tasks.get_task("model.dbt_test.model2").execution_airflow_task.upstream_task_ids
    )
    assert (
        "model2__ephemeral"
        in tasks.get_task("model.dbt_test.model1").test_airflow_task.downstream_task_ids
    )

    assert (
        "model2__ephemeral"
        in tasks.get_task("model.dbt_test.model3").execution_airflow_task.upstream_task_ids
    )
    assert (
        "model3__ephemeral"
        in tasks.get_task("model.dbt_test.model5").execution_airflow_task.downstream_task_ids
    )

    assert (
        "model3__ephemeral"
        in tasks.get_task("model.dbt_test.model10").execution_airflow_task.upstream_task_ids
    )
    assert (
        "model9__ephemeral"
        in tasks.get_task("model.dbt_test.model10").execution_airflow_task.upstream_task_ids
    )
    assert (
        "model10__ephemeral"
        in tasks.get_task("model.dbt_test.model3").execution_airflow_task.downstream_task_ids
    )
    assert (
        "model10__ephemeral"
        in tasks.get_task("model.dbt_test.model9").execution_airflow_task.downstream_task_ids
    )
    assert (
        "model11__ephemeral"
        in tasks.get_task("model.dbt_test.model10").execution_airflow_task.downstream_task_ids
    )
    assert (
        "model10__ephemeral"
        in tasks.get_task("model.dbt_test.model11").execution_airflow_task.upstream_task_ids
    )


def test_no_ephemeral_tasks():
    with test_dag():
        factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "no_ephemeral_operator")
        tasks = factory._builder.parse_manifest_into_tasks(factory._manifest_file_path())

    # then
    assert (
        task_group_prefix_builder("model1", "test")
        in tasks.get_task("model.dbt_test.model1").execution_airflow_task.downstream_task_ids
    )
    assert (
        task_group_prefix_builder("model1", "run")
        in tasks.get_task("model.dbt_test.model1").test_airflow_task.upstream_task_ids
    )

    assert (
        task_group_prefix_builder("model1", "test")
        in tasks.get_task("model.dbt_test.model4").execution_airflow_task.upstream_task_ids
    )
    assert (
        task_group_prefix_builder("model4", "run")
        in tasks.get_task("model.dbt_test.model1").test_airflow_task.downstream_task_ids
    )

    assert (
        task_group_prefix_builder("model6", "test")
        in tasks.get_task("model.dbt_test.model4").execution_airflow_task.upstream_task_ids
    )
    assert (
        task_group_prefix_builder("model4", "run")
        in tasks.get_task("model.dbt_test.model6").test_airflow_task.downstream_task_ids
    )
