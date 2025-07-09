import requests
from acr_admin.models import GeneralSetting

class ACRCloudUtils:
    @staticmethod
    def get_channel_name_by_id(pid: int, channel_id: int, access_token: str = None):
        """
        Fetches the channel list for the given project id (pid) from ACRCloud API,
        finds the channel with the given channel_id, and returns its name.
        If pid is invalid, returns error dict and 403 status code with a project permission message.
        If channel_id is not found, returns error dict and 403 status code with a channel not found message.
        On success, returns (channel_name, None).
        """
        url = f"https://api-v2.acrcloud.com/api/bm-bd-projects/{pid}/channels"
        if not access_token:
            settings = GeneralSetting.objects.first()
            if not settings or not settings.arc_cloud_api_key:
                return {"error": "ACRCloud API key not configured"}, 403
            access_token = settings.arc_cloud_api_key
        headers = {"Authorization": f"Bearer {access_token}"}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 403:
                return {"error": "You don't have permission to access this project (invalid project id)"}, 403
            response.raise_for_status()
            data = response.json().get("data", [])
            for channel in data:
                if channel.get("id") == channel_id:
                    return channel.get("name"), None
            # If channel_id not found
            return {"error": "Channel ID not found in this project"}, 403
        except Exception as e:
            # Optionally log the error
            return {"error": "You don't have permission to access this project (invalid project id)"}, 403
