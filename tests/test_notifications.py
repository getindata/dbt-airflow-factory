from os import path
from unittest.mock import MagicMock, patch

import airflow

if airflow.__version__.startswith("1."):
    from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
else:
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from dbt_airflow_factory.airflow_dag_factory import AirflowDagFactory
from dbt_airflow_factory.notifications.handler import NotificationHandlersFactory


def test_notification_callback_creation():
    # given
    factory = AirflowDagFactory(path.dirname(path.abspath(__file__)), "notifications")

    # when
    dag = factory.create()

    # then
    assert dag.default_args["on_failure_callback"]


@patch("airflow.hooks.base_hook.BaseHook.get_connection")
@patch("airflow.contrib.operators.slack_webhook_operator.SlackWebhookOperator.__new__")
def test_notification_send(mock_operator_init, mock_get_connection):
    # given
    notifications_config = AirflowDagFactory(
        path.dirname(path.abspath(__file__)), "notifications"
    ).airflow_config["failure_handlers"]
    factory = NotificationHandlersFactory()
    context = create_context()
    mock_get_connection.return_value = create_connection()
    mock_operator = MagicMock()
    mock_operator_init.return_value = mock_operator

    # when
    factory.create_failure_handler(notifications_config)(context)

    # then
    mock_operator_init.assert_called_once_with(
        SlackWebhookOperator,
        task_id="slack_failure_notification",
        message=":red_circle: Task Failed.\n"
        "*Task*: task_id\n"
        "*Dag*: dag_id\n"
        "*Execution Time*: some date\n"
        "*Log Url*: log_url",
        http_conn_id="slack_failure",
        webhook_token="test_password",
        username="test_login",
    )
    mock_operator.execute.assert_called_once_with(context=context)


def create_context():
    task_instance = MagicMock()
    task_instance.configure_mock(**{"task_id": "task_id", "dag_id": "dag_id", "log_url": "log_url"})
    return {"task_instance": task_instance, "execution_date": "some date"}


def create_connection():
    connection = MagicMock()
    connection.configure_mock(**{"login": "test_login", "password": "test_password"})
    return connection
