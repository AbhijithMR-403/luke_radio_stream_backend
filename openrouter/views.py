from requests import RequestException
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .services import OpenRouterService


class OpenRouterModelsAPIView(APIView):
    authentication_classes = []
    permission_classes = []

    def get(self, request):
        name = request.query_params.get("name", "").strip()

        try:
            models_data = OpenRouterService.list_models()
            filtered_models_data = OpenRouterService.filter_models_by_name(models_data, name)
            return Response(
                {
                    "success": True,
                    "name": name or None,
                    "data": filtered_models_data,
                },
                status=status.HTTP_200_OK,
            )
        except RequestException as exc:
            response = getattr(exc, "response", None)
            if response is not None:
                try:
                    upstream_error = response.json()
                except ValueError:
                    upstream_error = {"message": response.text}

                return Response(
                    {
                        "success": False,
                        "error": "Failed to fetch OpenRouter models",
                        "details": upstream_error,
                    },
                    status=response.status_code,
                )

            return Response(
                {
                    "success": False,
                    "error": "Unable to connect to OpenRouter",
                    "details": str(exc),
                },
                status=status.HTTP_502_BAD_GATEWAY,
            )
