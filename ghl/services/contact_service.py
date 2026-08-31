from typing import Optional, Dict, Any
from .ghl_service import GHL


def create_or_update_contact(
    email: str,
    link: str,
    name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create or update a contact in GHL, setting its set_url custom field to `link`.

    Uses GHL API v3's upsert endpoint (one call, create-or-update handled by GHL itself
    based on email match), rather than a separate search-then-create/update flow.

    Args:
        email: Email address of the contact (required)
        link: The link/URL string to set in the custom field (required)
        name: Name of the contact (optional, only used when creating new contact)

    Returns:
        Dictionary containing the API response from the upsert operation

    Raises:
        ValueError: If email or link is not provided
        requests.exceptions.RequestException: If the API request fails
    """
    if not email:
        raise ValueError("email is required")
    if not link:
        raise ValueError("link is required")

    if not name:
        name = email.split("@")[0]  # Use email prefix as default name

    ghl = GHL()
    return ghl.upsert_contact(email=email, link=link, name=name)
