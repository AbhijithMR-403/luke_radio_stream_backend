from django.urls import path

from .views import (
    SupportTicketListCreateAPIView,
    SupportTicketDetailAPIView,
    SupportTicketRespondAPIView,
)


urlpatterns = [
    path("support-tickets/", SupportTicketListCreateAPIView.as_view(), name="support-ticket-list-create"),
    path("support-tickets/<int:ticket_id>/", SupportTicketDetailAPIView.as_view(), name="support-ticket-detail"),
    path("support-tickets/<int:ticket_id>/respond/", SupportTicketRespondAPIView.as_view(), name="support-ticket-respond"),
]


