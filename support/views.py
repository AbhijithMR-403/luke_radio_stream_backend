from django.core.mail import send_mail
from django.conf import settings
from rest_framework import permissions, status
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import SupportTicket, SupportTicketResponse
from .serializer import SupportTicketSerializer, SupportTicketResponseSerializer


class SupportTicketListCreateAPIView(APIView):
    parser_classes = (MultiPartParser, FormParser)

    def get_permissions(self):
        if self.request.method == "GET":
            return [permissions.IsAuthenticated(), permissions.IsAdminUser()]
        return [permissions.IsAuthenticated()]

    def get(self, request):
        queryset = SupportTicket.objects.all().order_by("-created_at")

        class SupportTicketPagination(PageNumberPagination):
            page_size = 20
            page_size_query_param = "page_size"
            max_page_size = 100

        paginator = SupportTicketPagination()
        page = paginator.paginate_queryset(queryset, request, view=self)
        serializer = SupportTicketSerializer(page, many=True)
        return paginator.get_paginated_response(serializer.data)

    def post(self, request):
        serializer = SupportTicketSerializer(data=request.data, context={"request": request})
        if serializer.is_valid():
            ticket = serializer.save()
            output = SupportTicketSerializer(ticket)
            return Response(output.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)



class SupportTicketDetailAPIView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get_object(self, request, ticket_id):
        try:
            ticket = SupportTicket.objects.get(id=ticket_id)
        except SupportTicket.DoesNotExist:
            return None
        if request.user.is_staff or ticket.user_id == request.user.id:
            return ticket
        return None

    def get(self, request, ticket_id):
        ticket = self.get_object(request, ticket_id)
        if ticket is None:
            return Response({"detail": "Not found."}, status=status.HTTP_404_NOT_FOUND)
        serializer = SupportTicketSerializer(ticket)
        return Response(serializer.data)


class SupportTicketRespondAPIView(APIView):
    permission_classes = [permissions.IsAuthenticated, permissions.IsAdminUser]

    def post(self, request, ticket_id):
        try:
            ticket = SupportTicket.objects.get(id=ticket_id)
        except SupportTicket.DoesNotExist:
            return Response({"detail": "Not found."}, status=status.HTTP_404_NOT_FOUND)

        serializer = SupportTicketResponseSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        response = SupportTicketResponse.objects.create(
            ticket=ticket,
            responder=request.user,
            message=serializer.validated_data["message"],
        )

        # Email notification to ticket owner
        if getattr(ticket.user, "email", None):
            subject = f"Support Response: {ticket.subject}"
            body = (
                f"Hello,\n\nYour support ticket has a new response.\n\n"
                f"Subject: {ticket.subject}\n"
                f"Ticket ID: {ticket.id}\n\n"
                f"Question:\n{ticket.description}\n\n"
                f"Response:\n{response.message}\n\n"
                f"You can view the full conversation here: {settings.FRONTEND_URL}/support/tickets/{ticket.id}\n\n"
                f"Regards,\nSupport Team"
            )
            try:
                send_mail(subject, body, settings.EMAIL_HOST_USER, [ticket.user.email], fail_silently=True)
            except Exception:
                # Intentionally fail silently to not block API on email errors
                pass

        output = SupportTicketResponseSerializer(response)
        return Response(output.data, status=status.HTTP_201_CREATED)

