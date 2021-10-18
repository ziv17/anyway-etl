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
        },
    }
}

TYPES_MAPPING = {ALERTS: WazeAlert, JAMS: WazeTrafficJams}

QUERIES = {
    ALERTS: {
        "select_count": """
            SELECT COUNT(UUID)
            FROM waze_alerts 
            WHERE UUID = '{uuid}'
        """,
        "update": """
            UPDATE waze_alerts
            SET city='{city}',
                confidence={confidence},
                created_at='{created_at}',
                latitude={latitude},
                magvar={magvar},
                number_thumbs_up={number_thumbs_up},
                report_rating={report_rating},
                reliability={reliability},
                alert_type='{alert_type}',
                alert_subtype='{alert_subtype}',
                street='{street}',
                road_type={road_type},
                geom='{geom}',
                jam_uuid='{jam_uuid}',
                longitude={longitude},
                report_by_municipality_user={report_by_municipality_user},
                report_description='{report_description}',
                ended_at_estimate='{ended_at_estimate}',
                back_filled={back_filled},
                update_time='{update_time}'
            WHERE UUID='{uuid}'
        """,
        "insert": """
            INSERT INTO waze_alerts (
                city,    
                confidence,
                created_at,
                latitude,
                magvar,
                number_thumbs_up,
                report_rating,
                reliability,
                alert_type,
                alert_subtype,
                street,
                road_type,
                geom,
                jam_uuid,
                longitude,
                report_by_municipality_user,
                report_description,
                ended_at_estimate,
                back_filled,
                update_time,
                insertion_time
            )
            VALUES (
                '{city}',
                {confidence},
                '{created_at}',
                {latitude},
                {magvar},
                {number_thumbs_up},
                {report_rating},
                {reliability},
                '{alert_type}',
                '{alert_subtype}',
                '{street}',
                {road_type},
                '{geom}',
                '{jam_uuid}',
                {longitude},
                {report_by_municipality_user},
                '{report_description}',
                '{ended_at_estimate}',
                {back_filled},
                '{update_time}',
                '{insertion_time}'
            )
        """,
    },
    JAMS: {
        "select_count": """
            SELECT COUNT(UUID)
            FROM waze_traffic_jams 
            WHERE UUID = '{uuid}'
        """,
        "update": """
            UPDATE waze_traffic_jams
            SET level={level},
                line='{line}',
                speed_kmh={speed_kmh},
                turn_type={turn_type},
                length={length},
                type={type},
                speed={speed},
                segments='{segments}',
                road_type={road_type},
                delay={delay},
                street='{street}',
                city='{city}',
                end_node='{end_node}',
                blocking_alert_uuid='{blocking_alert_uuid}',
                start_node='{start_node}',
                created_at='{created_at}',
                geom='{geom}',
                ended_at_estimate='{ended_at_estimate}',
                back_filled={back_filled},
                update_time='{update_time}'
            WHERE UUID='{uuid}'
        """,
        "insert": """
            INSERT INTO waze_traffic_jams (
                level,    
                line,
                speed_kmh,
                turn_type,
                length,
                type,
                speed,
                segments,
                road_type,
                delay,
                street,
                city,
                end_node,
                blocking_alert_uuid,
                start_node,
                created_at,
                geom,
                ended_at_estimate,
                back_filled,
                update_time,
                insertion_time
            )
            VALUES (
                {level},
                '{line}',
                {speed_kmh},
                {turn_type},
                {length},
                {type},
                {speed},
                '{segments}',
                {road_type},
                {delay},
                '{street}',
                '{city}',
                '{end_node}',
                '{blocking_alert_uuid}',
                '{start_node}',
                '{created_at}',
                '{geom}',
                '{ended_at_estimate}',
                {back_filled},
                '{update_time}',
                '{insertion_time}'
            )
        """,
    },
}
