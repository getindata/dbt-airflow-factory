from collections.abc import Callable
from typing import Any
from urllib.parse import quote_plus

from dbt_airflow_factory.constants import (
    IS_AIRFLOW_NEWER_THAN_2_4,
    IS_FIRST_AIRFLOW_VERSION,
)

if IS_FIRST_AIRFLOW_VERSION:
    from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
else:
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

if IS_AIRFLOW_NEWER_THAN_2_4:
    from airflow.hooks.base import BaseHook
else:
    from airflow.hooks.base_hook import BaseHook

from dbt_airflow_factory.notifications.ms_teams_webhook_operator import (
    MSTeamsWebhookOperator,
)


class NotificationHandlersFactory:
    def create_failure_handler(self, handlers_config: dict) -> Callable:
        def failure_handler(context: Any) -> None:
            for handler_definition in handlers_config:
                if handler_definition["type"] == "slack":
                    connection = BaseHook.get_connection(handler_definition["connection_id"])
                    return SlackWebhookOperator(
                        task_id="slack_failure_notification",
                        message=handler_definition["message_template"].format(
                            task=context.get("task_instance").task_id,
                            dag=context.get("task_instance").dag_id,
                            execution_time=context.get("execution_date"),
                            url=context.get("task_instance").log_url,
                        ),
                        http_conn_id=handler_definition["connection_id"],
                        webhook_token=connection.password,
                        username=connection.login,
                    ).execute(context=context)
                elif handler_definition["type"] == "teams":
                    webserver_url = handler_definition["webserver_url"]
                    webserver_url = (
                        webserver_url[:-1] if webserver_url.endswith("/") else webserver_url
                    )
                    dag_id = context.get("task_instance").dag_id
                    task_id = context.get("task_instance").task_id
                    context["task_instance"].xcom_push(key=dag_id, value=True)
                    query = quote_plus(
                        f"log?dag_id={dag_id}&task_id={task_id}&execution_date={context['ts']}",
                        safe="=&?",
                    )
                    logs_url = f"{webserver_url}/{query}"

                    teams_notification = MSTeamsWebhookOperator(
                        task_id="teams_failure_notification",
                        message=handler_definition["message_template"].format(
                            task=task_id,
                            dag=dag_id,
                            execution_time=context.get("execution_date"),
                            url=logs_url,
                        ),
                        button_text="View log",
                        button_url=logs_url,
                        theme_color="FF0000",
                        http_conn_id=handler_definition["connection_id"],
                    )
                    return teams_notification.execute(context)

        return failure_handler
