# Create your models here.

from django.db import models
from django.contrib.auth.models import BaseUserManager, AbstractBaseUser
from django.utils import timezone
from datetime import timedelta
import random
import string
from acr_admin.models import Channel
from config.settings import AUTH_USER_MODEL

class RadioUserManager(BaseUserManager):
    def create_user(self, email, name, password=None):
        """
        Creates and saves a User with the given email, name and password.
        """
        if not email:
            raise ValueError("Users must have an email address")
        if not name:
            raise ValueError("Users must have a name")

        user = self.model(
            email=self.normalize_email(email),
            name=name,
        )

        if password:
            user.set_password(password)
            user.password_set = True
        
        user.save(using=self._db)
        return user

    def create_superuser(self, email, name, password=None):
        """
        Creates and saves a superuser with the given email, name and password.
        """
        user = self.create_user(
            email,
            name,
            password=password,
        )
        user.is_admin = True
        user.save(using=self._db)
        return user


class RadioUser(AbstractBaseUser):
    email = models.EmailField(
        verbose_name="email address",
        max_length=255,
        unique=True,
    )
    name = models.CharField(max_length=255, verbose_name="Full Name")
    is_active = models.BooleanField(default=True)
    is_admin = models.BooleanField(default=False)
    password_set = models.BooleanField(default=False)  # Track if user has set their password

    objects = RadioUserManager()

    USERNAME_FIELD = "email"
    REQUIRED_FIELDS = ["name"]

    def __str__(self):
        return self.email

    def has_perm(self, perm, obj=None):
        return True

    def has_module_perms(self, app_label):
        return True

    @property
    def is_staff(self):
        return self.is_admin


# Assignment of channels to users
class UserChannelAssignment(models.Model):
    user = models.ForeignKey('accounts.RadioUser', on_delete=models.CASCADE, related_name='channel_assignments')
    channel = models.ForeignKey(Channel, on_delete=models.CASCADE, related_name='user_assignments')
    assigned_by = models.ForeignKey('accounts.RadioUser', on_delete=models.SET_NULL, null=True, blank=True, related_name='assigned_channels')  # Admin who assigned
    assigned_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('user', 'channel')

    def __str__(self):
        return f"{self.user.email} -> {self.channel.name}"


class MagicLink(models.Model):
    user = models.ForeignKey(RadioUser, on_delete=models.CASCADE, related_name='magic_links')
    token = models.CharField(max_length=64, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()
    is_used = models.BooleanField(default=False)
    
    class Meta:
        ordering = ['-created_at']
    
    def save(self, *args, **kwargs):
        if not self.token:
            self.token = self.generate_token()
        if not self.expires_at:
            self.expires_at = timezone.now() + timedelta(hours=24)  # Magic link expires in 24 hours
        super().save(*args, **kwargs)
    
    def generate_token(self):
        """Generate a secure random token"""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=64))
    
    def is_valid(self):
        """Check if magic link is valid and not expired"""
        return not self.is_used and timezone.now() < self.expires_at
    
    def mark_as_used(self):
        """Mark magic link as used"""
        self.is_used = True
        self.save()
    
    def __str__(self):
        return f"Magic link for {self.user.email}: {self.token[:10]}..."
