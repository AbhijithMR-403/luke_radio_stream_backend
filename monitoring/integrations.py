import logging

import requests
from django.conf import settings

from core_admin.utils import BaseAPIUtils

logger = logging.getLogger(__name__)


def validate_openai_api_key(api_key: str, max_attempts: int = 2):
    """
    Validates that an OpenAI API key is both authenticated AND has active billing/quota.

    Deliberately makes a minimal real (billed) call - a 1-word embedding - rather than
    listing models: /v1/models only checks authentication and succeeds even for accounts
    with no payment method or exhausted credits, so it can't detect the "needs payment"
    case this check exists for. OpenAI has no dedicated billing-status endpoint; a small
    real request is the standard way to surface insufficient_quota.

    Returns dict: {"is_valid": bool, "error_message": str or None}
    """
    if not api_key or not api_key.strip():
        return {"is_valid": False, "error_message": "OpenAI API key cannot be empty"}

    url = "https://api.openai.com/v1/embeddings"
    headers = {"Authorization": f"Bearer {api_key.strip()}", "Content-Type": "application/json"}
    payload = {"model": "text-embedding-3-small", "input": "healthcheck"}

    response = None
    for attempt in range(max_attempts):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
        except requests.exceptions.RequestException:
            if attempt < max_attempts - 1:
                continue
            return {"is_valid": True, "error_message": None}  # fail open on network errors
        if response.status_code >= 500 and attempt < max_attempts - 1:
            continue
        break

    # Fail open on persistent server errors, matching ACRCloudUtils/RevAIUtils's philosophy.
    if response.status_code >= 500:
        return {"is_valid": True, "error_message": None}
    if response.status_code == 200:
        return {"is_valid": True, "error_message": None}
    if response.status_code in (401, 403, 429):
        # 401/403: bad key. 429: rate-limited OR insufficient_quota (no payment method/credits) -
        # OpenAI's error body distinguishes them and extract_error_message surfaces whichever it is.
        error_message = BaseAPIUtils.extract_error_message(response, "Invalid or rate-limited OpenAI API key")
        return {"is_valid": False, "error_message": error_message}

    return {"is_valid": False, "error_message": f"Unexpected response from OpenAI API: {response.status_code}"}


_GHL_BASE_URL = "https://services.leadconnectorhq.com"
_GHL_API_VERSION = "v3"


def send_alert_via_ghl(email: str, message: str, name: str | None = None):
    """
    Upserts a GHL contact (GHL API v3, one call handles create-or-update by email match)
    with `message` set on the alert custom field (settings.GHL_CUSTOM_FIELD_ALERT_MESSAGE).
    Reimplemented here (not imported from ghl/services) so the monitoring app doesn't need
    any changes to the ghl app to deliver alerts.
    """
    custom_field_id = getattr(settings, "GHL_CUSTOM_FIELD_ALERT_MESSAGE", "")
    print(f"Custom field ID: {custom_field_id}")
    api_key = getattr(settings, "GHL_API_KEY", "")
    location_id = getattr(settings, "GHL_LOCATION_ID", "")
    if not (custom_field_id and api_key and location_id):
        logger.warning("monitoring: GHL alert delivery not configured, message not sent: %s", message)
        return None

    headers = {
        "Content-Type": "application/json",
        "Version": _GHL_API_VERSION,
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    payload = {
        "email": email,
        "name": name or email.split("@")[0],
        "locationId": location_id,
        "customFields": [{"id": custom_field_id, "fieldValue": message}],
    }

    response = requests.post(f"{_GHL_BASE_URL}/contacts/upsert", headers=headers, json=payload)
    response.raise_for_status()
    return response.json()
