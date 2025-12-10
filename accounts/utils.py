from django.conf import settings
from django.utils import timezone
from .models import MagicLink
import logging

logger = logging.getLogger(__name__)

try:
    from ghl.services.contact_service import create_or_update_contact
except ImportError:
    # GHL service not available, create_or_update_contact will be None
    create_or_update_contact = None

def generate_and_send_magic_link(user):
    """
    Generate a new magic link for user and create/update contact in GHL with the magic link URL
    """
    # Check for existing active magic links and deactivate them
    existing_links = MagicLink.objects.filter(
        user=user,
        is_used=False,
        expires_at__gt=timezone.now()
    )
    
    if existing_links.exists():
        # Deactivate all existing active magic links
        existing_links.update(is_used=True)
    
    # Create new magic link
    magic_link = MagicLink.objects.create(user=user)
    
    # Create the magic link URL
    magic_link_url = f"{settings.FRONTEND_URL}/create-password?token={magic_link.token}"
    
    # Create or update contact in GHL with the magic link URL
    if create_or_update_contact:
        try:
            create_or_update_contact(
                email=user.email,
                link=magic_link_url,
                name=user.name
            )
            logger.info(f"GHL contact created/updated for {user.email}")
        except Exception as e:
            # Log error but don't fail the whole process if GHL fails
            logger.error(f"Failed to create/update GHL contact for {user.email}: {str(e)}")
    
    return magic_link
