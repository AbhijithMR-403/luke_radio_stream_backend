from django.core.exceptions import ValidationError
from acr_admin.models import GeneralSetting, Channel
from zoneinfo import ZoneInfo

class ValidationUtils:
    """Utility class for validating function calls and parameters"""
    
    @staticmethod
    def validate_channel_exists(project_id: int, channel_id: int):
        """Validate that the channel exists and is not deleted"""
        try:
            channel = Channel.objects.get(project_id=project_id, channel_id=channel_id, is_deleted=False)
            return channel
        except Channel.DoesNotExist:
            raise ValidationError(f"Channel with project_id {project_id} and channel_id {channel_id} not found or is deleted")
    
    @staticmethod
    def validate_settings_exist():
        """Validate that GeneralSetting exists"""
        settings = GeneralSetting.objects.first()
        if not settings:
            raise ValidationError("GeneralSetting not found. Please configure the application settings.")
        return settings
    
    @staticmethod
    def validate_acr_cloud_api_key():
        """Validate that ACRCloud API key is configured"""
        settings = ValidationUtils.validate_settings_exist()
        if not settings.acr_cloud_api_key:
            raise ValidationError("ACRCloud API key not configured in GeneralSetting")
        return settings.acr_cloud_api_key
    
    @staticmethod
    def validate_revai_api_key():
        """Validate that Rev.ai API key is configured"""
        settings = ValidationUtils.validate_settings_exist()
        if not settings.revai_access_token:
            raise ValidationError("Rev.ai API key not configured in GeneralSetting")
        return settings.revai_access_token
    
    @staticmethod
    def validate_openai_api_key():
        """Validate that OpenAI API key is configured"""
        settings = ValidationUtils.validate_settings_exist()
        if not settings.openai_api_key:
            raise ValidationError("OpenAI API key not configured in GeneralSetting")
        return settings.openai_api_key
    
    @staticmethod
    def validate_positive_integer(value, field_name: str):
        """Validate that a value is a positive integer"""
        if not isinstance(value, int) or value <= 0:
            raise ValidationError(f"{field_name} must be a positive integer, got: {value}")
        return value
    
    @staticmethod
    def validate_positive_number(value, field_name: str):
        """Validate that a value is a positive number"""
        if not isinstance(value, (int, float)) or value <= 0:
            raise ValidationError(f"{field_name} must be a positive number, got: {value}")
        return value
    
    @staticmethod
    def validate_required_field(value, field_name: str):
        """Validate that a required field is not None or empty"""
        if value is None or (isinstance(value, str) and not value.strip()):
            raise ValidationError(f"{field_name} is required and cannot be empty")
        return value
    
    @staticmethod
    def validate_list_not_empty(value, field_name: str):
        """Validate that a list is not empty"""
        if not isinstance(value, list):
            raise ValidationError(f"{field_name} must be a list, got: {type(value)}")
        if not value:
            raise ValidationError(f"{field_name} cannot be empty")
        return value
    
    @staticmethod
    def validate_file_path(file_path: str):
        """Validate that a file path is valid"""
        if not file_path or not isinstance(file_path, str):
            raise ValidationError("File path must be a non-empty string")
        if not file_path.startswith('/'):
            raise ValidationError("File path must start with '/'")
        return file_path
    
    @staticmethod
    def validate_url(url: str):
        """Validate that a URL is valid"""
        if not url or not isinstance(url, str):
            raise ValidationError("URL must be a non-empty string")
        if not url.startswith(('http://', 'https://')):
            raise ValidationError("URL must start with http:// or https://")
        return url
    
    @staticmethod
    def validate_timezone(timezone_str: str):
        """Validate that a timezone string is valid"""
        if not timezone_str or not isinstance(timezone_str, str):
            raise ValidationError("Timezone must be a non-empty string")
        
        try:
            ZoneInfo(timezone_str)
            return timezone_str
        except Exception:
            raise ValidationError(f"Invalid timezone: {timezone_str}") 