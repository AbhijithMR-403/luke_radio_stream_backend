import requests
from config.validation import ValidationUtils


class OpenAIUtils:
    @staticmethod
    def validate_api_key(api_key: str):
        """
        Validates OpenAI API key by calling the /v1/me endpoint.
        Returns (is_valid: bool, error_message: str or None, email: str or None)
        """
        if not api_key or not api_key.strip():
            return False, "OpenAI API key cannot be empty", None
        
        url = "https://api.openai.com/v1/me"
        headers = {
            "Authorization": f"Bearer {api_key.strip()}"
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                try:
                    user_data = response.json()
                    email = user_data.get('email', '')
                    return True, None, email
                except:
                    return True, None, None
            elif response.status_code == 401:
                try:
                    error_data = response.json()
                    error_message = error_data.get('error', {}).get('message', 'Invalid API key')
                    return False, error_message, None
                except:
                    return False, "Invalid API key provided", None
            else:
                return False, f"Unexpected response from OpenAI API: {response.status_code}", None
        except requests.exceptions.RequestException as e:
            return False, f"Failed to validate API key: {str(e)}", None


class ACRCloudUtils:
    @staticmethod
    def validate_api_key(api_key: str):
        """
        Validates ACR Cloud API key by calling the /api/bm-bd-projects endpoint.
        Returns (is_valid: bool, error_message: str or None)
        """
        if not api_key or not api_key.strip():
            return False, "ACR Cloud API key cannot be empty"
        
        url = "https://api-v2.acrcloud.com/api/bm-bd-projects"
        headers = {
            "Authorization": f"Bearer {api_key.strip()}"
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                return True, None
            elif response.status_code == 401 or response.status_code == 403:
                try:
                    error_data = response.json()
                    error_message = error_data.get('error', {}).get('message', 'Invalid API key')
                    if not error_message:
                        error_message = error_data.get('message', 'Invalid API key')
                    return False, error_message or 'Invalid ACR Cloud API key'
                except:
                    return False, "Invalid ACR Cloud API key provided"
            else:
                return False, f"Unexpected response from ACR Cloud API: {response.status_code}"
        except requests.exceptions.RequestException as e:
            return False, f"Failed to validate API key: {str(e)}"


class RevAIUtils:
    @staticmethod
    def validate_api_key(access_token: str):
        """
        Validates Rev.ai access token by calling the /speechtotext/v1/vocabularies endpoint.
        Returns (is_valid: bool, error_message: str or None)
        """
        if not access_token or not access_token.strip():
            return False, "Rev.ai access token cannot be empty"
        
        url = "https://api.rev.ai/speechtotext/v1/vocabularies?limit=0"
        headers = {
            "Authorization": f"Bearer {access_token.strip()}"
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                return True, None
            elif response.status_code == 401 or response.status_code == 403:
                try:
                    error_data = response.json()
                    error_message = error_data.get('error', {}).get('message', 'Invalid access token')
                    if not error_message:
                        error_message = error_data.get('message', 'Invalid access token')
                    return False, error_message or 'Invalid Rev.ai access token'
                except:
                    return False, "Invalid Rev.ai access token provided"
            else:
                return False, f"Unexpected response from Rev.ai API: {response.status_code}"
        except requests.exceptions.RequestException as e:
            return False, f"Failed to validate access token: {str(e)}"
    
    @staticmethod
    def get_channel_name_by_id(pid: int, channel_id, access_token: str = None):
        """
        Fetches the channel list for the given project id (pid) from ACRCloud API,
        finds the channel with the given channel_id, and returns its name.
        If pid is invalid, returns error dict and 403 status code with a project permission message.
        If channel_id is not found, returns error dict and 403 status code with a channel not found message.
        On success, returns (channel_name, None).
        """
        # Validate parameters
        ValidationUtils.validate_positive_integer(pid, "project_id")
        
        # Ensure channel_id is an integer, or return error if not valid
        try:
            channel_id_int = int(channel_id)
            ValidationUtils.validate_positive_integer(channel_id_int, "channel_id")
        except (ValueError, TypeError):
            return {"error": "Invalid channel ID. Must be an integer or string of digits."}, 400
        
        url = f"https://api-v2.acrcloud.com/api/bm-bd-projects/{pid}/channels"
        if not access_token:
            access_token = ValidationUtils.validate_acr_cloud_api_key()
        headers = {"Authorization": f"Bearer {access_token}"}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 403:
                return {"error": "You don't have permission to access this project (invalid project id)"}, 403
            response.raise_for_status()
            data = response.json().get("data", [])
            for channel in data:
                if channel.get("id") == channel_id_int:
                    return channel.get("name"), None
            # If channel_id not found
            return {"error": "Channel ID not found in this project"}, 403
        except Exception as e:
            # Optionally log the error
            return {"error": "You don't have permission to access this project"}, 403

