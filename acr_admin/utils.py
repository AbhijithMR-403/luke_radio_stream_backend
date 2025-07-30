import requests
from data_analysis.utils import ValidationUtils


class ACRCloudUtils:
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

