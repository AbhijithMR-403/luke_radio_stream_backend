from django.core.mail import send_mail
from django.conf import settings
from django.utils import timezone
from .models import MagicLink
import logging

logger = logging.getLogger(__name__)

def send_magic_link_email(user, magic_link):
    """
    Send magic link to user's email
    """
    try:
        # Create the magic link URL
        magic_link_url = f"{settings.FRONTEND_URL}/create-password?token={magic_link.token}"
        
        subject = 'Set Your Password - Radio Stream'
        message = f"""
        Hello {user.name},
        
        Click the link below to set your password:
        
        {magic_link_url}
        
        This link will expire in 24 hours.
        
        If you didn't request this, please ignore this email.
        
        Best regards,
        Radio Stream Team
        """
        print(message)
        send_mail(
            subject=subject,
            message=message,
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=[user.email],
            fail_silently=False,
        )
        logger.info(f"Magic link email sent to {user.email}")
        return True
    except Exception as e:
        logger.error(f"Failed to send magic link email to {user.email}: {str(e)}")
        return False

def generate_and_send_magic_link(user):
    """
    Generate a new magic link for user and send it via email
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
    
    # Send magic link via email
    success = send_magic_link_email(user, magic_link)
    
    if success:
        return magic_link
    else:
        # If email sending fails, delete the magic link
        magic_link.delete()
        return None
