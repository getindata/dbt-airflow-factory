"""
MS Teams webhook implementation.
"""

from typing import Any, Optional

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook


# Credits: https://code.mendhak.com/Airflow-MS-Teams-Operator/
class MSTeamsWebhookHook(HttpHook):
    """
    This hook allows you to post messages to MS Teams using the Incoming Webhook connector.

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
        **kwargs: Any
    ) -> None:
        super(MSTeamsWebhookHook, self).__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.webhook_token = self.get_token(webhook_token, http_conn_id)
        self.message = message
        self.subtitle = subtitle
        self.button_text = button_text
        self.button_url = button_url
        self.theme_color = theme_color
        self.proxy = proxy

    def get_proxy(self, http_conn_id: Any) -> str:
        """
        Return proxy address from connection object
        :param http_conn_id: The conn_id provided
        :return: proxy address (str) to use
        """
        conn = self.get_connection(http_conn_id)
        extra = conn.extra_dejson
        return extra.get("proxy", "")

    def get_token(self, token: Optional[str], http_conn_id: Optional[str]) -> str:
        """
        Given either a manually set token or a conn_id, return the webhook_token to use
        :param token: The manually provided token
        :param http_conn_id: The conn_id provided
        :return: webhook_token (str) to use
        """
        if token:
            return token

        if http_conn_id:
            conn = self.get_connection(http_conn_id)
            extra = conn.extra_dejson
            return extra.get("webhook_token", "")

        raise AirflowException("Cannot get URL: No valid MS Teams webhook URL nor conn_id supplied")

    def build_message(self) -> str:
        """
        Builds payload for MS Teams webhook.
        """
        card_json = """
                {{
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "{3}",
            "summary": "{0}",
            "sections": [{{
                "activityTitle": "{1}",
                "activitySubtitle": "{2}",
                "markdown": true,
                "potentialAction": [
                    {{
                        "@type": "OpenUri",
                        "name": "{4}",
                        "targets": [
                            {{ "os": "default", "uri": "{5}" }}
                        ]
                    }}
                ]
            }}]
            }}
                """
        return card_json.format(
            self.message,
            self.message,
            self.subtitle,
            self.theme_color,
            self.button_text,
            self.button_url,
        )

    def execute(self) -> None:
        """
        Remote Popen (actually execute the webhook call)

        :param cmd: command to remotely execute
        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        """
        proxies = {}
        proxy_url = self.get_proxy(self.http_conn_id)

        if len(proxy_url) > 5:
            proxies = {"https": proxy_url}

        self.run(
            endpoint=self.webhook_token,
            data=self.build_message(),
            headers={"Content-type": "application/json"},
            extra_options={"proxies": proxies},
        )
