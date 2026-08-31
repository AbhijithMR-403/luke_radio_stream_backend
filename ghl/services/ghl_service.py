import requests
from typing import Dict, Any, Optional
from django.conf import settings


class GHL:
    """
    Service class for interacting with GoHighLevel (GHL) API.
    Provides methods for contact management operations.
    """

    BASE_URL = "https://services.leadconnectorhq.com"
    API_VERSION = "v3"

    def __init__(self, location_id: Optional[str] = None, set_url_custom_id: Optional[str] = None, access_token: Optional[str] = None):
        """
        Initialize GHL service with location ID and custom field ID.

        Args:
            location_id: GHL location ID. If not provided, uses GHL_LOCATION_ID from settings.
            set_url_custom_id: Custom field ID for the URL field. If not provided, uses GHL_CUSTOM_FIELD_SET_URL from settings.
            access_token: GHL API access token (Bearer token). If not provided, uses GHL_API_KEY from settings.

        Raises:
            ValueError: If required settings (GHL_API_KEY, GHL_LOCATION_ID, GHL_CUSTOM_FIELD_SET_URL) are not configured.
        """
        # Use provided values or fall back to settings
        self.access_token = access_token or settings.GHL_API_KEY
        self.location_id = location_id or settings.GHL_LOCATION_ID
        self.set_url_custom_id = set_url_custom_id or settings.GHL_CUSTOM_FIELD_SET_URL

        # Validate that all required values are present
        if not self.access_token:
            raise ValueError("GHL_API_KEY must be set in settings or provided as access_token parameter")
        if not self.location_id:
            raise ValueError("GHL_LOCATION_ID must be set in settings or provided as location_id parameter")
        if not self.set_url_custom_id:
            raise ValueError("GHL_CUSTOM_FIELD_SET_URL must be set in settings or provided as set_url_custom_id parameter")

        self.contact_id = None

    def _get_headers(self) -> Dict[str, str]:
        """
        Get standard headers for GHL API requests.

        Returns:
            Dictionary of headers
        """
        return {
            "Content-Type": "application/json",
            "Version": self.API_VERSION,
            "Authorization": f"Bearer {self.access_token}"
        }

    def _get_headers_with_accept(self) -> Dict[str, str]:
        """
        Get headers for GHL API requests including Accept header.

        Returns:
            Dictionary of headers
        """
        headers = self._get_headers()
        headers["Accept"] = "application/json"
        return headers

    def upsert_contact(
        self,
        email: str,
        link: str,
        name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create-or-update a contact in one call (GHL API v3 POST /contacts/upsert),
        setting the link in the set_url custom field.

        Args:
            email: Email address of the contact
            link: The link/URL string to set in the custom field
            name: Name of the contact (used if a new contact is created)

        Returns:
            Dictionary containing the API response - {"new": bool, "contact": {...}, ...}

        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        url = f"{self.BASE_URL}/contacts/upsert"

        payload = {
            "email": email,
            "locationId": self.location_id,
            "customFields": [
                {
                    "id": self.set_url_custom_id,
                    "fieldValue": link
                }
            ]
        }
        if name:
            payload["name"] = name

        headers = self._get_headers_with_accept()

        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()

        return response.json()
