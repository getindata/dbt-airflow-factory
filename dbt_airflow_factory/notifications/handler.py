from collections.abc import Callable
from typing import Any
from urllib.parse import quote_plus

from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from dbt_airflow_factory.notifications.ms_teams_webhook_operator import (
    MSTeamsWebhookOperator,
)


class NotificationHandlersFactory:
    def create_failure_handler(self, handlers_config: dict) -> Callable:
        def failure_handler(context: Any) -> None:
            for handler_definition in handlers_config:
                if handler_definition["type"] == "slack":
                    connection = BaseHook.get_connection(handler_definition["connection_id"])
                    SlackWebhookOperator(
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
                    teams_notification.execute(context)
                elif handler_definition["type"] == "google_chat":
                    import json
                    import logging

                    webserver_url = handler_definition["webserver_url"]
                    webserver_url = (
                        webserver_url[:-1] if webserver_url.endswith("/") else webserver_url
                    )
                    dag_id = context.get("task_instance").dag_id
                    task_id = context.get("task_instance").task_id
                    query = quote_plus(
                        f"log?dag_id={dag_id}&task_id={task_id}&execution_date={context['ts']}",
                        safe="=&?",
                    )
                    logs_url = f"{webserver_url}/{query}"

                    message_text = handler_definition["message_template"].format(
                        task=task_id,
                        dag=dag_id,
                        execution_time=context.get("execution_date"),
                        url=logs_url,
                    )
                    message_body = {"text": message_text}

                    HttpHook(http_conn_id=handler_definition["connection_id"]).run(
                        data=json.dumps(message_body),
                        headers={"Content-Type": "application/json; charset=UTF-8"},
                    )
                    logging.info("Webhook request sent to Google Chat")

        return failure_handler
