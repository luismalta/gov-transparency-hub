import requests
from dagster import run_status_sensor, RunStatusSensorContext, DagsterRunStatus, EnvVar


class DiscordWebhook:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_message(self, content, username=None, avatar_url=None):
        """
        Sends a message to the Discord webhook.

        Args:
            content (str): The message content.
            username (str, optional): The username to display as the sender.
            avatar_url (str, optional): The avatar URL to display as the sender's avatar.

        Returns:
            tuple: A tuple containing the HTTP status code and the response text if the request is successful.
            None: If the request fails due to an exception.
        """
        if not content:
            raise ValueError("Content cannot be empty.")

        data = {
            "content": content
        }
        if username:
            data["username"] = username
        if avatar_url:
            data["avatar_url"] = avatar_url

        try:
            response = requests.post(self.webhook_url, json=data, timeout=5)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Failed to send message: {e}")
            return None
        return response.status_code, response.text
    
    def send_embeds(self, title, description, color, url, thumbnail_url, username, avatar_url):
        """
        Sends an embed message to a Discord webhook URL.

        Args:
            title (str): The title of the embed message.
            description (str): The description or body of the embed message.
            color (int): The color of the embed message in decimal format.
            url (str): The URL to be associated with the embed message.
            thumbnail_url (str): The URL of the thumbnail image to be displayed in the embed.
            username (str): The username to be displayed as the sender of the message.
            avatar_url (str): The URL of the avatar image to be displayed for the sender.

        Returns:
            tuple: A tuple containing the HTTP status code and the response text if the request is successful.
            None: If the request fails due to an exception.
        """
        data = {
            "embeds": [
                {
                    "description": description,
                    "color": color,
                    "title": title,
                    "url": url,
                    "thumbnail": {
                        "url": thumbnail_url
                    }
                }
            ],
            "username": username,
            "avatar_url": avatar_url
        }

        try:
            response = requests.post(self.webhook_url, json=data, timeout=5)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Failed to send message: {e}")
            return None
        return response.status_code, response.text

@run_status_sensor(run_status=DagsterRunStatus.FAILURE)
def discord_on_run_failure(
    context: RunStatusSensorContext,
):
    webhook = DiscordWebhook(EnvVar("DISCORD_WEBHOOK").get_value(""))

    message = (
        f"**Run ID:** {context.dagster_run.run_id}\n"
        f"**Job Name:** {context.dagster_run.job_name}\n"
        f"**Status:** {context.dagster_run.status.value}\n"
        f"**Partição:** {context.partition_key}\n"
    )

    webhook.send_embeds(
        title="Run Failure",
        description=message,
        color=15994896,
        url=EnvVar("DAGSTER_URL").get_value("https://localhost:3000") + f"/runs/{context.dagster_run.run_id}",
        thumbnail_url=EnvVar("DISCORD_THUMBNAIL_URL").get_value("https://dagster.io/images/brand/logos/dagster-primary-mark.png"),
        username=EnvVar("DISCORD_USERNAME").get_value("Dagster Bot"),
        avatar_url=EnvVar("DISCORD_AVATAR_URL").get_value("https://dagster.io/images/brand/logos/dagster-primary-mark.png")
    )
