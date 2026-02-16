import requests
from config.validation import ValidationUtils
from core_admin.models import GeneralSetting


class BaseAPIUtils:
    @staticmethod
    def _safe_request(url, headers, method="GET", timeout=10, max_attempts=2):
        """
        Wrapper around requests with simple retry logic.
        Returns a tuple of (status_or_error, response_or_none).
        - On success (non-5xx HTTP status): (status_code: int, response)
        - On persistent 5xx: ("SERVER_ERROR", None)
        - On persistent network error: ("NETWORK_ERROR", None)
        - Fallback: ("UNKNOWN_ERROR", None)
        """
        for attempt in range(max_attempts):
            try:
                response = requests.request(
                    method, url, headers=headers, timeout=timeout
                )

                # If 500+, retry. If last attempt, return 'SERVER_ERROR'
                if response.status_code >= 500:
                    if attempt < max_attempts - 1:
                        continue
                    return "SERVER_ERROR", None

                return response.status_code, response

            except requests.exceptions.RequestException:
                if attempt < max_attempts - 1:
                    continue
                return "NETWORK_ERROR", None

        return "UNKNOWN_ERROR", None

    @staticmethod
    def extract_error_message(response, default_message: str) -> str:
        """
        Safely parses a typical error JSON body and returns a message.
        Tries `error.message` first, then top-level `message`, otherwise
        falls back to the provided default_message.
        """
        try:
            data = response.json()
        except Exception:
            return default_message

        if isinstance(data, dict):
            msg = None
            error_obj = data.get("error")
            if isinstance(error_obj, dict):
                msg = error_obj.get("message")
            if not msg:
                msg = data.get("message")
            if msg:
                return msg

        return default_message


class OpenAIUtils:
    @staticmethod
    def validate_api_key(api_key: str):
        """
        Validates OpenAI API key by calling the /v1/me endpoint.
        Returns dict: {"is_valid": bool, "error_message": str or None, "email": str or None}
        """
        if not api_key or not api_key.strip():
            return {
                "is_valid": False,
                "error_message": "OpenAI API key cannot be empty",
                "email": None,
            }
        url = "https://api.openai.com/v1/me"
        headers = {"Authorization": f"Bearer {api_key.strip()}"}

        status, response = BaseAPIUtils._safe_request(
            url, headers=headers, method="GET", timeout=15
        )

        # Treat server/network issues as valid per requirements.
        if status in ("SERVER_ERROR", "NETWORK_ERROR", "UNKNOWN_ERROR"):
            return {
                "is_valid": True,
                "error_message": None,
                "email": None,
            }

        # At this point, status is an int and response is not None.
        if status == 200:
            try:
                user_data = response.json()
                email = user_data.get("email", "")
                return {
                    "is_valid": True,
                    "error_message": None,
                    "email": email,
                }
            except Exception:
                return {
                    "is_valid": True,
                    "error_message": None,
                    "email": None,
                }
        if status == 401:
            error_message = BaseAPIUtils.extract_error_message(
                response, "Invalid API key"
            )
            return {
                "is_valid": False,
                "error_message": error_message,
                "email": None,
            }

        return {
            "is_valid": False,
            "error_message": f"Unexpected response from OpenAI API: {status}",
            "email": None,
        }
    
    @staticmethod
    def validate_model(model: str, api_key: str):
        """
        Validates OpenAI model by calling the /v1/models/{model} endpoint.
        Returns dict: {"is_valid": bool, "error_message": str or None}
        """
        if not model or not model.strip():
            return {
                "is_valid": False,
                "error_message": "Model name cannot be empty",
            }
        
        if not api_key or not api_key.strip():
            return {
                "is_valid": False,
                "error_message": "OpenAI API key is required to validate the model",
            }
        url = f"https://api.openai.com/v1/models/{model.strip()}"
        headers = {"Authorization": f"Bearer {api_key.strip()}"}

        status, response = BaseAPIUtils._safe_request(
            url, headers=headers, method="GET", timeout=10
        )

        # Treat server/network issues as valid per requirements.
        if status in ("SERVER_ERROR", "NETWORK_ERROR", "UNKNOWN_ERROR"):
            return {"is_valid": True, "error_message": None}

        # At this point, status is an int and response is not None.
        if status == 200:
            return {"is_valid": True, "error_message": None}
        if status == 401:
            error_message = BaseAPIUtils.extract_error_message(
                response, "Invalid API key"
            )
            return {"is_valid": False, "error_message": error_message}
        if status == 404:
            # Model not found should definitively be treated as invalid.
            return {
                "is_valid": False,
                "error_message": f"Model '{model}' not found or not available with this API key",
            }

        return {
            "is_valid": False,
            "error_message": f"Unexpected response from OpenAI API: {status}",
        }


class ACRCloudUtils:
    @staticmethod
    def validate_api_key(api_key: str):
        """
        Validates ACR Cloud API key by calling the /api/bm-bd-projects endpoint.
        Returns dict: {"is_valid": bool, "error_message": str or None}
        """
        if not api_key or not api_key.strip():
            return {
                "is_valid": False,
                "error_message": "ACR Cloud API key cannot be empty",
            }
        url = "https://api-v2.acrcloud.com/api/bm-bd-projects"
        headers = {"Authorization": f"Bearer {api_key.strip()}"}

        status, response = BaseAPIUtils._safe_request(
            url, headers=headers, method="GET"
        )

        # Treat server/network issues as valid per requirements.
        if status in ("SERVER_ERROR", "NETWORK_ERROR", "UNKNOWN_ERROR"):
            return {"is_valid": True, "error_message": None}

        # At this point, status is an int and response is not None.
        if status == 200:
            return {"is_valid": True, "error_message": None}
        if status in (401, 403):
            error_message = BaseAPIUtils.extract_error_message(
                response, "Invalid API key"
            )
            return {
                "is_valid": False,
                "error_message": error_message or "Invalid ACR Cloud API key",
            }

        return {
            "is_valid": False,
            "error_message": f"Unexpected response from ACR Cloud API: {status}",
        }


class RevAIUtils:
    @staticmethod
    def validate_api_key(access_token: str):
        """
        Validates Rev.ai access token by calling the /speechtotext/v1/vocabularies endpoint.
        Returns dict: {"is_valid": bool, "error_message": str or None}
        """
        if not access_token or not access_token.strip():
            return {
                "is_valid": False,
                "error_message": "Rev.ai access token cannot be empty",
            }
        url = "https://api.rev.ai/speechtotext/v1/vocabularies?limit=0"
        headers = {"Authorization": f"Bearer {access_token.strip()}"}

        status, response = BaseAPIUtils._safe_request(
            url, headers=headers, method="GET", timeout=10
        )

        # Treat server/network issues as valid per requirements.
        if status in ("SERVER_ERROR", "NETWORK_ERROR", "UNKNOWN_ERROR"):
            return {"is_valid": True, "error_message": None}

        # At this point, status is an int and response is not None.
        if status == 200:
            return {"is_valid": True, "error_message": None}
        if status in (401, 403):
            error_message = BaseAPIUtils.extract_error_message(
                response, "Invalid Rev.ai access token"
            )
            return {
                "is_valid": False,
                "error_message": error_message or "Invalid Rev.ai access token",
            }

        return {
            "is_valid": False,
            "error_message": f"Unexpected response from Rev.ai API: {status}",
        }
    
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


# --- Default channel settings validation (for SetChannelDefaultSettings API) ---
DEFAULT_SETTINGS_REQUIRED_FIELDS = [
    'openai_api_key',
    'acr_cloud_api_key',
    'revai_access_token',
    'summarize_transcript_prompt',
    'sentiment_analysis_prompt',
    'general_topics_prompt',
    'iab_topics_prompt',
    'determine_radio_content_type_prompt',
    'content_type_prompt',
]


def channel_has_complete_settings(channel):
    """
    Return (ok: bool, missing_fields: list).
    Channel must have an active GeneralSetting with all DEFAULT_SETTINGS_REQUIRED_FIELDS non-empty.
    """
    active = GeneralSetting.objects.filter(channel=channel, is_active=True).first()
    if not active:
        return False, []
    missing = [
        f for f in DEFAULT_SETTINGS_REQUIRED_FIELDS
        if not (getattr(active, f, None) and str(getattr(active, f, '')).strip())
    ]
    return len(missing) == 0, missing
