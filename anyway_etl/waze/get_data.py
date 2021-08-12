import requests

from anyway_etl.waze.config import API_URL, API_PARAMS


def get_data_from_api() -> dict:
    response = requests.get(url=API_URL, params=API_PARAMS, verify=False)

    response_json = response.json()

    return response_json
