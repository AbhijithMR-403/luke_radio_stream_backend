import requests
from typing import Dict, Any, Optional, List
from django.conf import settings


class GHL:
    """
    Service class for interacting with GoHighLevel (GHL) API.
    Provides methods for contact management operations.
    """
    
    BASE_URL = "https://services.leadconnectorhq.com"
    API_VERSION = "2021-07-28"
    
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
    
    def search_contacts(
        self,
        filters: List[Dict[str, Any]],
        page: int = 1,
        page_limit: int = 20
    ) -> Dict[str, Any]:
        """
        Search for contacts in GHL based on provided filters.
        
        Args:
            filters: List of filter dictionaries with 'field', 'operator', and 'value' keys
                    Example: [{"field": "email", "operator": "eq", "value": "test@test.cm"}]
            page: Page number for pagination (default: 1)
            page_limit: Number of results per page (default: 20)
        
        Returns:
            Dictionary containing the API response
        
        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        url = f"{self.BASE_URL}/contacts/search"
        
        payload = {
            "locationId": self.location_id,
            "page": page,
            "pageLimit": page_limit,
            "filters": filters
        }
        
        headers = self._get_headers()
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        return response.json()
    
    def search_contact_by_email(
        self,
        email: str,
        page: int = 1,
        page_limit: int = 20
    ) -> Dict[str, Any]:
        """
        Search for a contact by email address.
        
        Args:
            email: Email address to search for
            page: Page number for pagination (default: 1)
            page_limit: Number of results per page (default: 20)
        
        Returns:
            Dictionary containing the API response
        
        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        filters = [
            {
                "field": "email",
                "operator": "eq",
                "value": email
            }
        ]
        
        return self.search_contacts(filters=filters, page=page, page_limit=page_limit).get("contacts", [])[0]
    
    def create_contact(
        self,
        name: str,
        email: str,
        link: str
    ) -> Dict[str, Any]:
        """
        Create a new contact in GHL with a link in the custom field.
        
        Args:
            name: Name of the contact
            email: Email address of the contact
            link: The link/URL string to set in the custom field
        
        Returns:
            Dictionary containing the API response
        
        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        url = f"{self.BASE_URL}/contacts/"
        
        payload = {
            "name": name,
            "email": email,
            "locationId": self.location_id,
            "customFields": [
                {
                    "id": self.set_url_custom_id,
                    "field_value": link
                }
            ]
        }
        
        headers = self._get_headers_with_accept()
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        return response.json()
    
    def update_contact(
        self,
        link: str
    ) -> Dict[str, Any]:
        """
        Update a contact in GHL with a link in the custom field.
        
        Args:
            link: The link/URL string to set in the custom field
        
        Returns:
            Dictionary containing the API response
        
        Raises:
            requests.exceptions.RequestException: If the API request fails
            ValueError: If contact_id is not set on the instance
        """
        if not self.contact_id:
            raise ValueError("contact_id must be set on the GHL instance before calling update_contact")
        
        url = f"{self.BASE_URL}/contacts/{self.contact_id}"
        
        payload = {
            "customFields": [
                {
                    "id": self.set_url_custom_id,
                    "field_value": link
                }
            ]
        }
        
        headers = self._get_headers_with_accept()
        
        response = requests.put(url, headers=headers, json=payload)
        response.raise_for_status()
        
        return response.json()

