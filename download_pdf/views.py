import os
import tempfile
import uuid

from django.conf import settings
from django.http import HttpResponse
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated

from .service import generate_multi_page_pdf


def _get_bearer_token(request):
    """Extract Bearer token from Authorization header."""
    auth = (request.META.get("HTTP_AUTHORIZATION") or "").strip()
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    return auth if auth else None


class DashboardPdfDownloadView(APIView):
    """
    Generate and download a multi-page dashboard PDF. Uses FRONTEND_URL from .env.
    Uses the same JWT from the request's Authorization header for frontend access.

    Request body (JSON):
        - channelId (required): channel identifier
        - channelName (optional): display name for the channel
        - slides (optional): list of slide indices, e.g. [0, 1]. Defaults to [0, 1].
        - start_time (optional): e.g. "2025-01-01"
        - end_time (optional): e.g. "2025-01-31"
        - shift_id (optional): e.g. 5
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        data = request.data or {}
        channel_id = data.get("channelId")
        channel_name = data.get("channelName", "")
        slides = data.get("slides")
        start_time = data.get("start_time") or None
        end_time = data.get("end_time") or None
        shift_id = data.get("shift_id")

        if not channel_id:
            return Response(
                {"success": False, "error": "channelId is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        access_token = _get_bearer_token(request)
        if not access_token:
            return Response(
                {"success": False, "error": "Authorization header with Bearer token is required"},
                status=status.HTTP_401_UNAUTHORIZED,
            )

        base_url = getattr(settings, "FRONTEND_URL", "").rstrip("/")
        if not base_url:
            return Response(
                {"success": False, "error": "FRONTEND_URL is not configured"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        if slides is not None and not isinstance(slides, list):
            return Response(
                {"success": False, "error": "slides must be a list of integers"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if slides is not None:
            try:
                slides = [int(s) for s in slides]
            except (TypeError, ValueError):
                return Response(
                    {"success": False, "error": "slides must be a list of integers"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        pdf_filename = f"dashboard_{channel_id}_{uuid.uuid4().hex[:8]}.pdf"
        pdf_path = os.path.join(tempfile.gettempdir(), pdf_filename)

        try:
            generate_multi_page_pdf(
                base_url=base_url,
                pdf_path=pdf_path,
                access_token=access_token,
                channel_id=str(channel_id),
                channel_name=channel_name,
                slides=slides,
                start_time=str(start_time) if start_time else None,
                end_time=str(end_time) if end_time else None,
                shift_id=shift_id,
            )
        except Exception as e:
            return Response(
                {"success": False, "error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        try:
            with open(pdf_path, "rb") as f:
                pdf_bytes = f.read()
        finally:
            try:
                os.remove(pdf_path)
            except OSError:
                pass

        response = HttpResponse(pdf_bytes, content_type="application/pdf")
        response["Content-Disposition"] = f'attachment; filename="{pdf_filename}"'
        return response
