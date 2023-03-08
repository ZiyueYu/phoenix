import base64
import os


class Authentication:
    @staticmethod
    def get_authentication_base_64_encoded_str() -> str:
        user = os.environ["MBUTEST_APIUSER"]
        password = os.environ["MBUTEST_APIPWD"]
        authentication_str = f"{user}:{password}"
        authentication_base64_encoded_str = base64.b64encode(
            authentication_str.encode("ascii"),
        ).decode("ascii")
        return authentication_base64_encoded_str

    @staticmethod
    def get_api_key() -> str:
        api_key = os.environ["MBUTEST_APIKEY"]
        return api_key
