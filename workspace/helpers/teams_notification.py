"""
IRP Notebook Framework - Microsoft Teams Notification Helper

Provides functionality to send notifications to Microsoft Teams
via incoming webhooks with support for:
- Simple text messages with different styles (success, warning, error, info)
- Adaptive Cards with formatted content
- Configurable action buttons
- Retry logic for reliability

Environment Variables:
    TEAMS_WEBHOOK_URL: The Microsoft Teams webhook URL
    TEAMS_NOTIFICATION_ENABLED: Enable/disable notifications (default: true)
    TEAMS_DEFAULT_DASHBOARD_URL: Default dashboard URL for action buttons
    TEAMS_DEFAULT_JUPYTERLAB_URL: Default JupyterLab URL for action buttons

Example:
    >>> from helpers.teams_notification import TeamsNotificationClient
    >>> client = TeamsNotificationClient()
    >>> client.send_success("Deployment Complete", "Version 2.3 deployed successfully")
"""

import os
import logging
from typing import Dict, List, Any, Optional

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


# Configure module logger
logger = logging.getLogger(__name__)


# ============================================================================
# EXCEPTIONS
# ============================================================================

class TeamsNotificationError(Exception):
    """Base exception for all Teams notification errors."""
    pass


class TeamsWebhookError(TeamsNotificationError):
    """Webhook URL configuration or request errors."""
    pass


class TeamsValidationError(TeamsNotificationError):
    """Input validation errors."""
    pass


# ============================================================================
# CONSTANTS
# ============================================================================

# Notification styles using Adaptive Card predefined colors
# Maps style name to display properties
NOTIFICATION_STYLES = {
    "Attention": {"emoji": "✗", "description": "errors/critical"},
    "Warning": {"emoji": "⚠", "description": "warnings"},
    "Good": {"emoji": "✓", "description": "info/success"},
    "Accent": {"emoji": "ℹ", "description": "general alerts"},
    "Default": {"emoji": "•", "description": "neutral"},
}


# ============================================================================
# CLIENT CLASS
# ============================================================================

class TeamsNotificationClient:
    """
    Client for sending notifications to Microsoft Teams via webhooks.

    This client supports sending Adaptive Cards with different notification
    styles, custom action buttons, and includes retry logic for reliability.

    Attributes:
        webhook_url: The Teams webhook URL
        enabled: Whether notifications are enabled
        timeout: Request timeout in seconds
        default_dashboard_url: Default URL for dashboard action button
        default_jupyterlab_url: Default URL for JupyterLab action button

    Example:
        >>> client = TeamsNotificationClient()
        >>> client.send_success("Build Complete", "All tests passed!")
        True

        >>> # With custom webhook URL
        >>> client = TeamsNotificationClient(webhook_url="https://...")
        >>> client.send_error("Build Failed", "See logs for details")
        True
    """

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        enabled: Optional[bool] = None,
        timeout: int = 30,
        max_retries: int = 3
    ):
        """
        Initialize the Teams notification client.

        Args:
            webhook_url: Teams webhook URL. If not provided, reads from
                        TEAMS_WEBHOOK_URL environment variable.
            enabled: Enable/disable notifications. If not provided, reads from
                    TEAMS_NOTIFICATION_ENABLED environment variable (default: true).
            timeout: Request timeout in seconds.
            max_retries: Maximum number of retry attempts for failed requests.
        """
        # Load configuration from environment or parameters
        self.webhook_url = webhook_url or os.environ.get('TEAMS_WEBHOOK_URL')

        if enabled is not None:
            self.enabled = enabled
        else:
            self.enabled = os.environ.get(
                'TEAMS_NOTIFICATION_ENABLED', 'true'
            ).lower() == 'true'

        self.timeout = timeout

        # Default action button URLs
        self.default_dashboard_url = os.environ.get(
            'TEAMS_DEFAULT_DASHBOARD_URL', ''
        )
        self.default_jupyterlab_url = os.environ.get(
            'TEAMS_DEFAULT_JUPYTERLAB_URL', ''
        )

        # Setup session with retry logic
        self.session = requests.Session()
        retry = Retry(
            total=max_retries,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=["POST"],
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))

        logger.debug(
            f"TeamsNotificationClient initialized: enabled={self.enabled}, "
            f"webhook_url={'[SET]' if self.webhook_url else '[NOT SET]'}"
        )

    def _validate_style(self, style: str) -> None:
        """
        Validate that the notification style is supported.

        Args:
            style: The style name to validate

        Raises:
            TeamsValidationError: If the style is not supported
        """
        if style not in NOTIFICATION_STYLES:
            valid_styles = ", ".join(NOTIFICATION_STYLES.keys())
            raise TeamsValidationError(
                f"Invalid style '{style}'. Choose from: {valid_styles}"
            )

    def _build_adaptive_card(
        self,
        style: str,
        title: str,
        message: str,
        actions: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """
        Build the Adaptive Card payload for Teams.

        Args:
            style: Notification style (Attention, Warning, Good, Accent, Default)
            title: Title of the notification
            message: Message body (supports Markdown)
            actions: List of action buttons with 'title' and 'url' keys

        Returns:
            The complete message payload for Teams webhook
        """
        emoji = NOTIFICATION_STYLES[style]["emoji"]

        # Build action buttons
        card_actions = []
        if actions:
            for action in actions:
                card_actions.append({
                    "type": "Action.OpenUrl",
                    "title": action.get("title", "Open"),
                    "url": action.get("url", "")
                })
        elif self.default_dashboard_url or self.default_jupyterlab_url:
            # Use default actions if no custom actions provided
            if self.default_dashboard_url:
                card_actions.append({
                    "type": "Action.OpenUrl",
                    "title": "View Dashboard",
                    "url": self.default_dashboard_url
                })
            if self.default_jupyterlab_url:
                card_actions.append({
                    "type": "Action.OpenUrl",
                    "title": "Launch JupyterLab",
                    "url": self.default_jupyterlab_url
                })

        # Build Adaptive Card content
        card_content = {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [
                {
                    "type": "TextBlock",
                    "text": f"{emoji} {title}",
                    "weight": "Bolder",
                    "size": "Medium",
                    "color": style
                },
                {
                    "type": "TextBlock",
                    "text": message,
                    "wrap": True
                }
            ]
        }

        # Add actions if any
        if card_actions:
            card_content["actions"] = card_actions

        # Build complete payload
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": card_content
                }
            ]
        }

        return payload

    def send_notification(
        self,
        style: str,
        title: str,
        message: str,
        actions: Optional[List[Dict[str, str]]] = None
    ) -> bool:
        """
        Send a notification to Microsoft Teams via webhook.

        Args:
            style: One of "Attention", "Warning", "Good", "Accent", "Default"
            title: Title of the notification
            message: Message body (Markdown supported)
            actions: Optional list of action buttons, each with 'title' and 'url'

        Returns:
            True if notification was sent successfully, False otherwise

        Raises:
            TeamsValidationError: If style is invalid
            TeamsWebhookError: If webhook URL is not configured

        Example:
            >>> client.send_notification(
            ...     style="Good",
            ...     title="Build Complete",
            ...     message="All **tests** passed!",
            ...     actions=[
            ...         {"title": "View Results", "url": "https://ci.example.com/123"}
            ...     ]
            ... )
            True
        """
        # Check if notifications are enabled
        if not self.enabled:
            logger.info("Notifications are disabled, skipping send")
            return True

        # Validate webhook URL
        if not self.webhook_url:
            raise TeamsWebhookError(
                "Webhook URL not configured. Set TEAMS_WEBHOOK_URL environment "
                "variable or pass webhook_url to constructor."
            )

        # Validate style
        self._validate_style(style)

        # Build payload
        payload = self._build_adaptive_card(style, title, message, actions)

        # Send notification
        try:
            response = self.session.post(
                self.webhook_url,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()

            logger.info(
                f"Notification sent successfully: style={style}, title='{title}'"
            )
            return True

        except requests.exceptions.Timeout:
            logger.error(f"Timeout sending notification: {self.timeout}s exceeded")
            return False

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send notification: {e}")
            return False

    def send_success(
        self,
        title: str,
        message: str,
        actions: Optional[List[Dict[str, str]]] = None
    ) -> bool:
        """
        Send a success notification (green checkmark).

        Args:
            title: Title of the notification
            message: Message body
            actions: Optional action buttons

        Returns:
            True if sent successfully
        """
        return self.send_notification("Good", title, message, actions)

    def send_warning(
        self,
        title: str,
        message: str,
        actions: Optional[List[Dict[str, str]]] = None
    ) -> bool:
        """
        Send a warning notification (yellow warning symbol).

        Args:
            title: Title of the notification
            message: Message body
            actions: Optional action buttons

        Returns:
            True if sent successfully
        """
        return self.send_notification("Warning", title, message, actions)

    def send_error(
        self,
        title: str,
        message: str,
        actions: Optional[List[Dict[str, str]]] = None
    ) -> bool:
        """
        Send an error notification (red X).

        Args:
            title: Title of the notification
            message: Message body
            actions: Optional action buttons

        Returns:
            True if sent successfully
        """
        return self.send_notification("Attention", title, message, actions)

    def send_info(
        self,
        title: str,
        message: str,
        actions: Optional[List[Dict[str, str]]] = None
    ) -> bool:
        """
        Send an info notification (blue info symbol).

        Args:
            title: Title of the notification
            message: Message body
            actions: Optional action buttons

        Returns:
            True if sent successfully
        """
        return self.send_notification("Accent", title, message, actions)


# ============================================================================
# ACTION BUTTON HELPERS
# ============================================================================

def build_notification_actions(
    notebook_path: str = '',
    cycle_name: str = '',
    schema: str = 'public'
) -> List[Dict[str, str]]:
    """
    Build standard action buttons for IRP workflow notifications.

    Creates action buttons for:
    - "Open Notebook": Link to the notebook in JupyterLab
    - "View Cycle Dashboard": Link to the cycle-specific dashboard page

    Args:
        notebook_path: Path to notebook file (for "Open Notebook" button)
        cycle_name: Cycle name (for cycle-specific dashboard link)
        schema: Database schema for dashboard URL

    Returns:
        List of action button dicts with 'title' and 'url' keys

    Example:
        >>> actions = build_notification_actions(
        ...     notebook_path='/workspace/workflows/Active_Q1/notebooks/Stage_03/Step_01.ipynb',
        ...     cycle_name='Q1-2025',
        ...     schema='production'
        ... )
        >>> # Returns: [{"title": "Open Notebook", "url": "..."}, {"title": "View Cycle Dashboard", "url": "..."}]
    """
    actions = []

    # JupyterLab notebook link
    base_url = os.environ.get('TEAMS_DEFAULT_JUPYTERLAB_URL', '')
    if base_url and notebook_path and 'workflows' in notebook_path:
        rel_path = notebook_path.split('workflows')[-1].lstrip('/\\')
        notebook_url = f"{base_url.rstrip('/')}/lab/tree/workspace/workflows/{rel_path}"
        actions.append({"title": "Open Notebook", "url": notebook_url})

    # Cycle-specific dashboard link
    dashboard_url = os.environ.get('TEAMS_DEFAULT_DASHBOARD_URL', '')
    if dashboard_url:
        if cycle_name and cycle_name != "Unknown":
            cycle_dashboard_url = f"{dashboard_url.rstrip('/')}/{schema}/cycle/{cycle_name}"
            actions.append({"title": "View Cycle Dashboard", "url": cycle_dashboard_url})
        else:
            actions.append({"title": "View Dashboard", "url": dashboard_url})

    return actions


def truncate_error(error: str, max_length: int = 500) -> str:
    """
    Truncate an error message for notification display.

    Args:
        error: The error message to truncate
        max_length: Maximum length before truncation (default: 500)

    Returns:
        Truncated error message with "..." suffix if truncated
    """
    if len(error) > max_length:
        return error[:max_length] + "..."
    return error


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def send_teams_notification(
    style: str,
    title: str,
    message: str,
    webhook_url: Optional[str] = None,
    actions: Optional[List[Dict[str, str]]] = None
) -> bool:
    """
    Send a notification to Microsoft Teams (convenience function).

    This function creates a temporary client and sends the notification.
    For multiple notifications, create a TeamsNotificationClient instance instead.

    Args:
        style: One of "Attention", "Warning", "Good", "Accent", "Default"
        title: Title of the notification
        message: Message body (Markdown supported)
        webhook_url: Optional webhook URL (defaults to TEAMS_WEBHOOK_URL env var)
        actions: Optional list of action buttons

    Returns:
        True if notification was sent successfully

    Example:
        >>> send_teams_notification(
        ...     "Good",
        ...     "Deployment Complete",
        ...     "Version 2.3 deployed successfully"
        ... )
        True
    """
    client = TeamsNotificationClient(webhook_url=webhook_url)
    return client.send_notification(style, title, message, actions)


def send_validation_failure_notification(
    cycle_name: str,
    stage_name: str,
    step_name: str,
    validation_errors: List[str],
    notebook_path: Optional[str] = None
) -> bool:
    """
    Send a Teams notification for validation failures in a notebook.

    Use this when a notebook's validation fails and the step will be marked
    as FAILED. This provides a clean, user-friendly notification.

    Args:
        cycle_name: Name of the cycle (e.g., "Analysis-2025-Q1")
        stage_name: Name of the stage (e.g., "Stage_03_Data_Import")
        step_name: Name of the step (e.g., "Step_02_Create_Base_Portfolios")
        validation_errors: List of validation error messages
        notebook_path: Optional path to notebook for action buttons

    Returns:
        True if notification was sent successfully

    Example:
        >>> send_validation_failure_notification(
        ...     cycle_name="Q1-2025",
        ...     stage_name="Stage_03_Data_Import",
        ...     step_name="Step_02_Create_Base_Portfolios",
        ...     validation_errors=["Portfolio 'ABC' already exists in EDM1"]
        ... )
        True
    """
    from helpers.database import get_current_schema

    client = TeamsNotificationClient()

    # Build error summary
    error_summary = "\n".join(f"• {e}" for e in validation_errors[:10])
    if len(validation_errors) > 10:
        error_summary += f"\n... and {len(validation_errors) - 10} more"

    # Build action buttons if notebook path provided
    actions = None
    if notebook_path:
        schema = get_current_schema()
        actions = build_notification_actions(notebook_path, cycle_name, schema)

    return client.send_error(
        title=f"[{cycle_name}] Validation Failed: {step_name}",
        message=f"**Cycle:** {cycle_name}\n"
                f"**Stage:** {stage_name}\n"
                f"**Step:** {step_name}\n\n"
                f"**Validation Errors:**\n{error_summary}",
        actions=actions
    )


# ============================================================================
# MAIN (for testing)
# ============================================================================

if __name__ == "__main__":
    # Configure logging for testing
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Create client
    client = TeamsNotificationClient()

    # Check configuration
    if not client.webhook_url:
        print("ERROR: TEAMS_WEBHOOK_URL environment variable not set")
        print("Please set it to your Teams webhook URL")
        exit(1)

    print("Sending test notifications to Teams...")

    # Send test notifications
    client.send_error(
        "System Failure",
        "**Critical error occurred in service X**"
    )

    client.send_warning(
        "High Memory Usage",
        "Memory usage exceeded 80% threshold"
    )

    client.send_success(
        "Deployment Complete",
        "Version 2.3 deployed successfully"
    )

    client.send_info(
        "Reminder",
        "Daily backup completed successfully"
    )

    print("All notifications sent!")
