import requests
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from config.validation import ValidationUtils
from .models import ACRCloudCustomFileUpload


class ACRCloudFileUploadView(APIView):
    """
    API endpoint to upload files to ACR Cloud.
    Only authenticated users can access this endpoint.
    
    Query parameters:
    - bucket_id: The ACR Cloud bucket ID
    - url: The audio URL to upload
    - title: Optional title for the file
    """
    permission_classes = [IsAuthenticated]
    
    def post(self, request):
        # Get query parameters
        bucket_id = request.query_params.get('bucket_id')
        audio_url = request.query_params.get('url')
        title = request.query_params.get('title')
        
        # Validate required parameters
        if not bucket_id:
            return Response(
                {'error': 'bucket_id query parameter is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        if not audio_url:
            return Response(
                {'error': 'url query parameter is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Validate URL format
        try:
            ValidationUtils.validate_url(audio_url)
        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Get ACR Cloud API token
        try:
            acr_token = ValidationUtils.validate_acr_cloud_api_key()
        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
        # Prepare request data
        request_data = {
            'url': audio_url,
            'data_type': 'audio_url'
        }
        
        # Add title if provided
        if title:
            request_data['title'] = title
        
        # Make request to ACR Cloud API
        acr_url = f'https://api-v2.acrcloud.com/api/buckets/{bucket_id}/files'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {acr_token}'
        }
        
        try:
            response = requests.post(
                acr_url,
                json=request_data,
                headers=headers,
                timeout=30
            )
            
            # Return the response from ACR Cloud
            try:
                response_data = response.json()
            except:
                response_data = {'raw_response': response.text}
            
            # Determine status based on response
            upload_status = 'success' if response.status_code in [200, 201] else 'failed'
            error_message = None
            if upload_status == 'failed':
                error_message = response_data.get('error', response_data.get('message', 'Upload failed'))
            
            # Save upload details to database
            ACRCloudCustomFileUpload.objects.create(
                bucket_id=bucket_id,
                audio_url=audio_url,
                title=title,
                status=upload_status,
                error_message=error_message,
                created_by=request.user if request.user.is_authenticated else None
            )
            
            return Response(
                response_data,
                status=response.status_code
            )
            
        except requests.exceptions.RequestException as e:
            error_msg = f'Failed to communicate with ACR Cloud API: {str(e)}'
            
            # Save error details to database
            ACRCloudCustomFileUpload.objects.create(
                bucket_id=bucket_id,
                audio_url=audio_url,
                title=title,
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
    
    Returns the list of buckets from ACR Cloud API.
    """
    permission_classes = [IsAuthenticated]
    
    def get(self, request):
        # Get ACR Cloud API token
        try:
            acr_token = ValidationUtils.validate_acr_cloud_api_key()
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
