from django.conf import settings
from django.db import models


class SupportTicket(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="support_tickets")
    subject = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.subject} ({self.user})"


class SupportTicketImage(models.Model):
    ticket = models.ForeignKey(SupportTicket, on_delete=models.CASCADE, related_name="images")
    image = models.ImageField(upload_to="support/")
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Image for ticket #{self.ticket_id}"



class SupportTicketResponse(models.Model):
    ticket = models.ForeignKey(SupportTicket, on_delete=models.CASCADE, related_name="responses")
    responder = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="support_ticket_responses")
    message = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["created_at"]

    def __str__(self):
        return f"Response by {self.responder} on ticket #{self.ticket_id}"
