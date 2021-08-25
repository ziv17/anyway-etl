from anyway.models import WazeAlert, WazeTrafficJams

ISRAEL_POLYGON = "33.662000,32.556000;34.722000,33.004000;35.793000,33.370000;35.914000,32.953000;35.765000,32.733000;35.600000,32.628000;35.473000,31.073000;35.230000,30.290000;34.985000,29.513000;34.898000,29.483000;33.662000,32.556000"

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
        },
    }
}

TYPES_MAPPING = {ALERTS: WazeAlert, JAMS: WazeTrafficJams}
