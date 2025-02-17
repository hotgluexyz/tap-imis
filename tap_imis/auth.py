import requests
from singer_sdk import typing as th


class IMISAuth(requests.auth.AuthBase):
    def __init__(self, config):
        self.config = config
        self.access_token = self.get_token()

    def get_token(self):
        site_url = self.config["site_url"]
        username = self.config['username']
        password = self.config['password']

        url = f"{site_url}/Token"

        payload = f"grant_type=password&username={username}&password={password}"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        response = requests.request("POST", url, headers=headers, data=payload)
        response = response.json()

        return response["access_token"]

    def __call__(self):
        return self.access_token
    

