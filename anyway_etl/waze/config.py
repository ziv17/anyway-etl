from datetime import datetime
from typing import Union

from anyway.models import WazeAlert, WazeTrafficJams

ISRAEL_POLYGON = "34.123,31.4;34.722,33.004;35.793,33.37;35.914,32.953;35.765,32.733;35.6,32.628;35.473,31.073;35.23,30.29;34.985,29.513;34.898,29.483;34.123,31.4"

API_URL = "https://il-georss.waze.com/rtserver/web/TGeoRSS"

ALERTS = "alerts"

JAMS = "jams"

FIELDS = [ALERTS, JAMS]


API_PARAMS = {
    "format": "JSON",
    "tk": "ccp_partner",
    "ccp_partner_name": "The Public Knowledge Workshop",
    "types": "traffic,alerts,irregularities",
    "polygon": ISRAEL_POLYGON,
}


def _convert_to_bool(value):
    if isinstance(value, bool):
        return value
    else:
        return str(value).lower() in ("yes", "true", "t", "1")


COLUMNS_MAPPING = {
    ALERTS: {
        "road_type": {"default": -1, "conversion": lambda value: value, "type": int},
        "number_thumbs_up": {
            "default": 0,
            "conversion": lambda value: value,
            "type": int,
        },
        "report_by_municipality_user": {
            "default": False,
            "conversion": _convert_to_bool,
            "type": bool,
        }
    }
}

WAZE_TYPE = Union[WazeAlert, WazeTrafficJams]

TYPES_MAPPING = {ALERTS: WazeAlert, JAMS: WazeTrafficJams}

QUERIES = {
    ALERTS: {
        "select_count": """
            SELECT COUNT(UUID)
            FROM waze_alerts
            WHERE UUID = '{uuid}'
        """
    },
    JAMS: {
        "select_count": """
            SELECT COUNT(UUID)
            FROM waze_traffic_jams
            WHERE UUID = '{uuid}'
        """
    },
}
