import requests
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from django.db import transaction
from config.validation import ValidationUtils
from .models import ACRCloudCustomFileUpload


class ACRCloudFileUploadView(APIView):

    permission_classes = [IsAuthenticated]

    def post(self, request):
        # Get query parameters
        bucket_id = request.query_params.get('bucket_id')
        title = request.query_params.get('title')
        channel_id = request.query_params.get('channel_id')
        uploaded_file = request.FILES.get('file')

        if not channel_id:
            return Response(
                {'error': 'channel_id query parameter is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        if not bucket_id:
            return Response(
                {'error': 'bucket_id query parameter is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        if not uploaded_file:
            return Response(
                {'error': 'file is required; upload a media file in the request body'},
                status=status.HTTP_400_BAD_REQUEST
            )

        file_name = getattr(uploaded_file, 'name', None) or 'upload'

        # Get ACR Cloud API token
        try:
            acr_token = ValidationUtils.validate_acr_cloud_api_key(channel_id)
        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        acr_url = f'https://api-v2.acrcloud.com/api/buckets/{bucket_id}/files'
        headers = {'Authorization': f'Bearer {acr_token}'}
        payload = {'data_type': 'audio'}
        if title:
            payload['title'] = title
        files = [
            ('file', (file_name, uploaded_file, getattr(uploaded_file, 'content_type', None) or 'application/octet-stream'))
        ]

        try:
            response = requests.post(
                acr_url,
                headers=headers,
                data=payload,
                files=files,
                timeout=60
            )

            try:
                response_data = response.json()
            except Exception:
                response_data = {'raw_response': response.text}

            upload_status = 'success' if response.status_code in [200, 201] else 'failed'
            error_message = None
            if upload_status == 'failed':
                error_message = response_data.get('error', response_data.get('message', 'Upload failed'))

            with transaction.atomic():
                ACRCloudCustomFileUpload.objects.create(
                    bucket_id=bucket_id,
                    audio_url=None,
                    file_name=file_name,
                    title=title or None,
                    status=upload_status,
                    error_message=error_message,
                    created_by=request.user if request.user.is_authenticated else None
                )

            return Response(response_data, status=response.status_code)

        except requests.exceptions.RequestException as e:
            error_msg = f'Failed to communicate with ACR Cloud API: {str(e)}'
            with transaction.atomic():
                ACRCloudCustomFileUpload.objects.create(
                    bucket_id=bucket_id,
                    audio_url=None,
                    file_name=file_name,
                    title=title or None,
                    status='error',
                    error_message=error_msg,
                    created_by=request.user if request.user.is_authenticated else None
                )
            return Response(
                {'error': error_msg},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ACRCloudBucketsView(APIView):
    """
    API endpoint to fetch buckets from ACR Cloud.
    Only authenticated users can access this endpoint.
    
    Query parameters:
    - channel_id: The channel ID (required) to resolve settings.
    
    Returns the list of buckets from ACR Cloud API.
    """
    permission_classes = [IsAuthenticated]
    
    def get(self, request):
        channel_id = request.query_params.get('channel_id')
        if not channel_id:
            return Response(
                {'error': 'channel_id query parameter is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        try:
            channel_id_int = int(channel_id)
        except (ValueError, TypeError):
            return Response(
                {'error': 'channel_id must be a valid integer'},
                status=status.HTTP_400_BAD_REQUEST
            )
        # Get ACR Cloud API token
        try:
            acr_token = ValidationUtils.validate_acr_cloud_api_key(channel_id_int)
        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
        # Make request to ACR Cloud API
        acr_url = 'https://api-v2.acrcloud.com/api/buckets'
        headers = {
            'Authorization': f'Bearer {acr_token}'
        }
        
        try:
            response = requests.get(
                acr_url,
                headers=headers,
                timeout=30
            )
            
            # Return the response from ACR Cloud
            try:
                response_data = response.json()
            except:
                response_data = {'raw_response': response.text}
            
            return Response(
                response_data,
                status=response.status_code
            )
            
        except requests.exceptions.RequestException as e:
            return Response(
                {'error': f'Failed to communicate with ACR Cloud API: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
