from typing import Optional, Dict, Any
from .ghl_service import GHL


def create_or_update_contact(
    email: str,
    link: str,
    name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create or update a contact in GHL.
    
    First searches for a contact by email. If found, updates the contact's
    set_url custom field. If not found, creates a new contact with the
    provided email, name, and link.
    
    Args:
        email: Email address of the contact (required)
        link: The link/URL string to set in the custom field (required)
        name: Name of the contact (optional, only used when creating new contact)
    
    Returns:
        Dictionary containing the API response from create or update operation
    
    Raises:
        ValueError: If email or link is not provided
        requests.exceptions.RequestException: If the API request fails
    """
    if not email:
        raise ValueError("email is required")
    if not link:
        raise ValueError("link is required")
    
    # Initialize GHL service
    ghl = GHL()
    
    try:
        # Try to find existing contact by email
        contact = ghl.search_contact_by_email(email)
        
        # Contact found - update it
        if contact and isinstance(contact, dict) and contact.get("id"):
            ghl.contact_id = contact["id"]
            return ghl.update_contact(link)
        else:
            # Contact not found in response - create new one
            if not name:
                name = email.split("@")[0]  # Use email prefix as default name
            return ghl.create_contact(name=name, email=email, link=link)
    
    except (IndexError, KeyError, TypeError, AttributeError):
        # Contact not found - create new one
        if not name:
            name = email.split("@")[0]  # Use email prefix as default name
        return ghl.create_contact(name=name, email=email, link=link)

