from collections.abc import Callable
from typing import Any

import airflow

if airflow.__version__.startswith("1."):
    from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
else:
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook


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

        return failure_handler
