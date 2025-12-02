"""
Test suite for Teams notification helper.

Tests cover:
- Successful notification sending
- All notification styles
- Retry logic on server errors
- Input validation
- Configuration validation
- Enable/disable toggle
- Custom action buttons
- Convenience methods and functions
"""

import pytest
import requests
import responses
from unittest.mock import patch, MagicMock

from helpers.teams_notification import (
    TeamsNotificationClient,
    TeamsNotificationError,
    TeamsWebhookError,
    TeamsValidationError,
    send_teams_notification,
    NOTIFICATION_STYLES,
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_env(monkeypatch):
    """Mock environment variables for testing."""
    monkeypatch.setenv('TEAMS_WEBHOOK_URL', 'https://webhook.test.com/teams')
    monkeypatch.setenv('TEAMS_NOTIFICATION_ENABLED', 'true')
    monkeypatch.setenv('TEAMS_DEFAULT_DASHBOARD_URL', 'https://dashboard.test.com')
    monkeypatch.setenv('TEAMS_DEFAULT_JUPYTERLAB_URL', 'https://jupyter.test.com')


@pytest.fixture
def client(mock_env):
    """Create client with mocked environment."""
    return TeamsNotificationClient()


@pytest.fixture
def webhook_url():
    """Return test webhook URL."""
    return 'https://webhook.test.com/teams'


# ============================================================================
# CLIENT INITIALIZATION TESTS
# ============================================================================

@pytest.mark.unit
def test_client_init_from_environment(mock_env):
    """Test client initializes from environment variables."""
    client = TeamsNotificationClient()

    assert client.webhook_url == 'https://webhook.test.com/teams'
    assert client.enabled is True
    assert client.default_dashboard_url == 'https://dashboard.test.com'
    assert client.default_jupyterlab_url == 'https://jupyter.test.com'


@pytest.mark.unit
def test_client_init_with_parameters():
    """Test client initializes with explicit parameters."""
    client = TeamsNotificationClient(
        webhook_url='https://custom.webhook.com',
        enabled=False,
        timeout=60,
        max_retries=5
    )

    assert client.webhook_url == 'https://custom.webhook.com'
    assert client.enabled is False
    assert client.timeout == 60


@pytest.mark.unit
def test_client_init_defaults_enabled_true(monkeypatch):
    """Test notifications are enabled by default."""
    monkeypatch.delenv('TEAMS_NOTIFICATION_ENABLED', raising=False)
    client = TeamsNotificationClient(webhook_url='https://test.com')

    assert client.enabled is True


@pytest.mark.unit
def test_client_init_disabled_from_env(monkeypatch):
    """Test notifications can be disabled via environment."""
    monkeypatch.setenv('TEAMS_NOTIFICATION_ENABLED', 'false')
    client = TeamsNotificationClient(webhook_url='https://test.com')

    assert client.enabled is False


# ============================================================================
# NOTIFICATION SENDING TESTS
# ============================================================================

@pytest.mark.unit
@responses.activate
def test_send_notification_success(client, webhook_url):
    """Test successful notification sending."""
    responses.add(
        responses.POST,
        webhook_url,
        status=200,
        body='1'  # Teams returns '1' on success
    )

    result = client.send_notification(
        style="Good",
        title="Test Title",
        message="Test message"
    )

    assert result is True
    assert len(responses.calls) == 1


@pytest.mark.unit
@responses.activate
def test_send_notification_payload_structure(client, webhook_url):
    """Test that the payload has correct Adaptive Card structure."""
    responses.add(responses.POST, webhook_url, status=200, body='1')

    client.send_notification(
        style="Good",
        title="Test Title",
        message="Test message"
    )

    # Verify payload structure
    request_body = responses.calls[0].request.body
    import json
    payload = json.loads(request_body)

    assert payload['type'] == 'message'
    assert len(payload['attachments']) == 1

    attachment = payload['attachments'][0]
    assert attachment['contentType'] == 'application/vnd.microsoft.card.adaptive'

    card = attachment['content']
    assert card['type'] == 'AdaptiveCard'
    assert card['version'] == '1.4'
    assert len(card['body']) == 2

    # Check title block
    title_block = card['body'][0]
    assert title_block['type'] == 'TextBlock'
    assert 'Test Title' in title_block['text']
    assert title_block['color'] == 'Good'

    # Check message block
    message_block = card['body'][1]
    assert message_block['text'] == 'Test message'
    assert message_block['wrap'] is True


@pytest.mark.unit
@responses.activate
def test_send_notification_all_styles(client, webhook_url):
    """Test notification sending with all supported styles."""
    for style in NOTIFICATION_STYLES.keys():
        responses.add(responses.POST, webhook_url, status=200, body='1')

    for style in NOTIFICATION_STYLES.keys():
        result = client.send_notification(
            style=style,
            title=f"Test {style}",
            message=f"Message for {style}"
        )
        assert result is True

    assert len(responses.calls) == len(NOTIFICATION_STYLES)


@pytest.mark.unit
@responses.activate
def test_send_notification_with_custom_actions(client, webhook_url):
    """Test notification with custom action buttons."""
    responses.add(responses.POST, webhook_url, status=200, body='1')

    custom_actions = [
        {"title": "View Logs", "url": "https://logs.example.com"},
        {"title": "Retry Job", "url": "https://ci.example.com/retry"}
    ]

    result = client.send_notification(
        style="Good",
        title="Test",
        message="Test",
        actions=custom_actions
    )

    assert result is True

    # Verify custom actions in payload
    import json
    payload = json.loads(responses.calls[0].request.body)
    card = payload['attachments'][0]['content']

    assert len(card['actions']) == 2
    assert card['actions'][0]['title'] == 'View Logs'
    assert card['actions'][1]['url'] == 'https://ci.example.com/retry'


@pytest.mark.unit
@responses.activate
def test_send_notification_with_default_actions(client, webhook_url):
    """Test notification uses default actions when none provided."""
    responses.add(responses.POST, webhook_url, status=200, body='1')

    client.send_notification(
        style="Good",
        title="Test",
        message="Test"
    )

    import json
    payload = json.loads(responses.calls[0].request.body)
    card = payload['attachments'][0]['content']

    # Should have default dashboard and jupyterlab actions
    assert len(card['actions']) == 2
    assert card['actions'][0]['title'] == 'View Dashboard'
    assert card['actions'][1]['title'] == 'Launch JupyterLab'


@pytest.mark.unit
@responses.activate
def test_send_notification_no_actions_when_defaults_empty(monkeypatch, webhook_url):
    """Test no actions included when defaults are empty."""
    monkeypatch.setenv('TEAMS_WEBHOOK_URL', webhook_url)
    monkeypatch.setenv('TEAMS_NOTIFICATION_ENABLED', 'true')
    monkeypatch.delenv('TEAMS_DEFAULT_DASHBOARD_URL', raising=False)
    monkeypatch.delenv('TEAMS_DEFAULT_JUPYTERLAB_URL', raising=False)

    client = TeamsNotificationClient()
    responses.add(responses.POST, webhook_url, status=200, body='1')

    client.send_notification(
        style="Good",
        title="Test",
        message="Test"
    )

    import json
    payload = json.loads(responses.calls[0].request.body)
    card = payload['attachments'][0]['content']

    # Should have no actions
    assert 'actions' not in card


# ============================================================================
# CONVENIENCE METHOD TESTS
# ============================================================================

@pytest.mark.unit
@responses.activate
def test_send_success(client, webhook_url):
    """Test send_success convenience method."""
    responses.add(responses.POST, webhook_url, status=200, body='1')

    result = client.send_success("Success Title", "Success message")

    assert result is True

    import json
    payload = json.loads(responses.calls[0].request.body)
    title_block = payload['attachments'][0]['content']['body'][0]
    assert title_block['color'] == 'Good'
    assert '✓' in title_block['text']


@pytest.mark.unit
@responses.activate
def test_send_warning(client, webhook_url):
    """Test send_warning convenience method."""
    responses.add(responses.POST, webhook_url, status=200, body='1')

    result = client.send_warning("Warning Title", "Warning message")

    assert result is True

    import json
    payload = json.loads(responses.calls[0].request.body)
    title_block = payload['attachments'][0]['content']['body'][0]
    assert title_block['color'] == 'Warning'


@pytest.mark.unit
@responses.activate
def test_send_error(client, webhook_url):
    """Test send_error convenience method."""
    responses.add(responses.POST, webhook_url, status=200, body='1')

    result = client.send_error("Error Title", "Error message")

    assert result is True

    import json
    payload = json.loads(responses.calls[0].request.body)
    title_block = payload['attachments'][0]['content']['body'][0]
    assert title_block['color'] == 'Attention'


@pytest.mark.unit
@responses.activate
def test_send_info(client, webhook_url):
    """Test send_info convenience method."""
    responses.add(responses.POST, webhook_url, status=200, body='1')

    result = client.send_info("Info Title", "Info message")

    assert result is True

    import json
    payload = json.loads(responses.calls[0].request.body)
    title_block = payload['attachments'][0]['content']['body'][0]
    assert title_block['color'] == 'Accent'


# ============================================================================
# CONVENIENCE FUNCTION TESTS
# ============================================================================

@pytest.mark.unit
@responses.activate
def test_send_teams_notification_function(mock_env, webhook_url):
    """Test the convenience function."""
    responses.add(responses.POST, webhook_url, status=200, body='1')

    result = send_teams_notification(
        style="Good",
        title="Test",
        message="Test message"
    )

    assert result is True


@pytest.mark.unit
@responses.activate
def test_send_teams_notification_function_with_custom_url():
    """Test convenience function with custom webhook URL."""
    custom_url = 'https://custom.webhook.com'
    responses.add(responses.POST, custom_url, status=200, body='1')

    result = send_teams_notification(
        style="Good",
        title="Test",
        message="Test message",
        webhook_url=custom_url
    )

    assert result is True


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================

@pytest.mark.unit
def test_invalid_style_raises_error(client):
    """Test that invalid style raises validation error."""
    with pytest.raises(TeamsValidationError) as exc_info:
        client.send_notification(
            style="InvalidStyle",
            title="Test",
            message="Test"
        )

    assert "Invalid style" in str(exc_info.value)
    assert "InvalidStyle" in str(exc_info.value)


@pytest.mark.unit
def test_missing_webhook_url_raises_error(monkeypatch):
    """Test that missing webhook URL raises error."""
    # Clear the environment variable to ensure it's not set
    monkeypatch.delenv('TEAMS_WEBHOOK_URL', raising=False)

    client = TeamsNotificationClient(webhook_url=None, enabled=True)

    with pytest.raises(TeamsWebhookError) as exc_info:
        client.send_notification(
            style="Good",
            title="Test",
            message="Test"
        )

    assert "Webhook URL not configured" in str(exc_info.value)


@pytest.mark.unit
@responses.activate
def test_http_error_returns_false(client, webhook_url):
    """Test that HTTP errors return False instead of raising."""
    responses.add(responses.POST, webhook_url, status=400, body='Bad Request')

    result = client.send_notification(
        style="Good",
        title="Test",
        message="Test"
    )

    assert result is False


@pytest.mark.unit
@responses.activate
def test_network_error_returns_false(client, webhook_url):
    """Test that network errors return False."""
    responses.add(
        responses.POST,
        webhook_url,
        body=requests.exceptions.ConnectionError("Connection failed")
    )

    result = client.send_notification(
        style="Good",
        title="Test",
        message="Test"
    )

    assert result is False


# ============================================================================
# RETRY LOGIC TESTS
# ============================================================================

@pytest.mark.unit
@responses.activate
def test_retry_on_500_error(client, webhook_url):
    """Test that client retries on 500 errors."""
    # First request fails, second succeeds
    responses.add(responses.POST, webhook_url, status=500)
    responses.add(responses.POST, webhook_url, status=200, body='1')

    result = client.send_notification(
        style="Good",
        title="Test",
        message="Test"
    )

    assert result is True
    assert len(responses.calls) == 2


@pytest.mark.unit
@responses.activate
def test_retry_on_503_error(client, webhook_url):
    """Test that client retries on 503 errors."""
    responses.add(responses.POST, webhook_url, status=503)
    responses.add(responses.POST, webhook_url, status=503)
    responses.add(responses.POST, webhook_url, status=200, body='1')

    result = client.send_notification(
        style="Good",
        title="Test",
        message="Test"
    )

    assert result is True
    assert len(responses.calls) == 3


@pytest.mark.unit
@responses.activate
def test_max_retries_exceeded(client, webhook_url):
    """Test that client fails after max retries."""
    # Add more failures than max_retries (default is 3)
    for _ in range(5):
        responses.add(responses.POST, webhook_url, status=500)

    result = client.send_notification(
        style="Good",
        title="Test",
        message="Test"
    )

    # Should fail after 4 attempts (1 original + 3 retries)
    assert result is False
    assert len(responses.calls) == 4


# ============================================================================
# DISABLE/ENABLE TESTS
# ============================================================================

@pytest.mark.unit
def test_disabled_notification_skipped(monkeypatch):
    """Test that disabled notifications are skipped."""
    monkeypatch.setenv('TEAMS_WEBHOOK_URL', 'https://webhook.test.com')

    client = TeamsNotificationClient(enabled=False)

    # Should return True without making any request
    result = client.send_notification(
        style="Good",
        title="Test",
        message="Test"
    )

    assert result is True


@pytest.mark.unit
@responses.activate
def test_enabled_notification_sent(client, webhook_url):
    """Test that enabled notifications are sent."""
    responses.add(responses.POST, webhook_url, status=200, body='1')

    result = client.send_notification(
        style="Good",
        title="Test",
        message="Test"
    )

    assert result is True
    assert len(responses.calls) == 1


# ============================================================================
# MARKDOWN SUPPORT TESTS
# ============================================================================

@pytest.mark.unit
@responses.activate
def test_markdown_in_message(client, webhook_url):
    """Test that Markdown formatting is preserved in message."""
    responses.add(responses.POST, webhook_url, status=200, body='1')

    markdown_message = "**Bold** and *italic* with `code`"

    client.send_notification(
        style="Good",
        title="Test",
        message=markdown_message
    )

    import json
    payload = json.loads(responses.calls[0].request.body)
    message_block = payload['attachments'][0]['content']['body'][1]

    assert message_block['text'] == markdown_message


# ============================================================================
# INTEGRATION TEST MARKERS
# ============================================================================

@pytest.mark.integration
@pytest.mark.skip(reason="Requires real Teams webhook URL")
def test_real_notification():
    """
    Integration test with real Teams webhook.

    To run this test:
    1. Set TEAMS_WEBHOOK_URL environment variable
    2. Run: pytest -m integration --run-integration
    """
    import os

    webhook_url = os.environ.get('TEAMS_WEBHOOK_URL')
    if not webhook_url:
        pytest.skip("TEAMS_WEBHOOK_URL not set")

    client = TeamsNotificationClient()

    result = client.send_success(
        "Integration Test",
        "This is a test notification from pytest"
    )

    assert result is True
