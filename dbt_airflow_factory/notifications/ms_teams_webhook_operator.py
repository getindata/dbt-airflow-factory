"""
MS Teams webhook operator.
"""

import logging
from typing import Any, Optional

from dbt_airflow_factory.constants import IS_FIRST_AIRFLOW_VERSION

if IS_FIRST_AIRFLOW_VERSION:
    from airflow.providers.http.operators.http import SimpleHttpOperator
else:
    from airflow.operators.http_operator import SimpleHttpOperator

from airflow.utils.decorators import apply_defaults

from dbt_airflow_factory.notifications.ms_teams_webhook_hook import MSTeamsWebhookHook


# Credits: https://code.mendhak.com/Airflow-MS-Teams-Operator/
class MSTeamsWebhookOperator(SimpleHttpOperator):
    """
    This operator allows you to post messages to MS Teams using the Incoming Webhooks connector.
    Takes both MS Teams webhook token directly and connection that has MS Teams webhook token.
    If both supplied, the webhook token will be appended to the host in the connection.

    :param http_conn_id: connection that has MS Teams webhook URL
    :type http_conn_id: str
    :param webhook_token: MS Teams webhook token
    :type webhook_token: str
    :param message: The message you want to send on MS Teams
    :type message: str
    :param subtitle: The subtitle of the message to send
    :type subtitle: str
    :param button_text: The text of the action button
    :type button_text: str
    :param button_url: The URL for the action button click
    :type button_url : str
    :param theme_color: Hex code of the card theme, without the #
    :type message: str
    :param proxy: Proxy to use when making the webhook request
    :type proxy: str
    """

    template_fields = (
        "message",
        "subtitle",
    )

    @apply_defaults
    def __init__(
        self,
        http_conn_id: Optional[str] = None,
        webhook_token: Optional[str] = None,
        message: str = "",
        subtitle: str = "",
        button_text: str = "",
        button_url: str = "",
        theme_color: str = "00FF00",
        proxy: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super(MSTeamsWebhookOperator, self).__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.webhook_token = webhook_token
        self.message = message
        self.subtitle = subtitle
        self.button_text = button_text
        self.button_url = button_url
        self.theme_color = theme_color
        self.proxy = proxy

    def execute(self, context: Any) -> None:
        """
        Call the webhook with the required parameters
        """
        MSTeamsWebhookHook(
            self.http_conn_id,
            self.webhook_token,
            self.message,
            self.subtitle,
            self.button_text,
            self.button_url,
            self.theme_color,
            self.proxy,
        ).execute()
        logging.info("Webhook request sent to MS Teams")
