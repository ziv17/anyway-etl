import json
import math
import datetime

import pandas as pd

from .config import CBS_ACCIDENT_MARKERS_ROOT_PATH
from .parse_common import coordinates_converter, get_data_value, common_main
from .parse_localization import (
    get_localization_city_name, get_supported_localization_tables,
    get_localization_field
)


def get_street(yishuv_symbol, street_sign, streets):
    """
    extracts the street name using the settlement id and street id
    """
    if yishuv_symbol not in streets:
        # Changed to return blank string instead of None for correct presentation (Omer)
        return ""
    street_name = [
        x["SHEM_RECHOV"]
        for x in streets[yishuv_symbol]
        if x["SEMEL_RECHOV"] == street_sign
    ]
    # there should be only one street name, or none if it wasn't found.
    return street_name[0] if len(street_name) == 1 else ""


def get_address(accident, streets):
    """
    extracts the address of the main street.
    tries to build the full address: <street_name> <street_number>, <settlement>,
    but might return a partial one if unsuccessful.
    """
    street = get_street(
        accident.get("SEMEL_YISHUV"), accident.get("REHOV1"), streets
    )
    if not street:
        return ""

    # the house_number field is invalid if it's empty or if it contains 9999
    house_number = (
        int(accident.get("BAYIT"))
        if not pd.isnull(accident.get("BAYIT"))
        and int(accident.get("BAYIT")) != 9999
        else None
    )
    settlement = get_localization_city_name(accident.get("SEMEL_YISHUV"))

    if not house_number and not settlement:
        return street
    if not house_number and settlement:
        return "{}, {}".format(street, settlement)
    if house_number and not settlement:
        return "{} {}".format(street, house_number)

    return "{} {}, {}".format(street, house_number, settlement)


def get_streets(accident, streets):
    """
    extracts the streets the accident occurred in.
    every accident has a main street and a secondary street.
    :return: a tuple containing both streets.
    """
    main_street = get_address(accident, streets)
    secondary_street = get_street(
        accident.get("SEMEL_YISHUV"), accident.get("REHOV2"), streets
    )
    return main_street, secondary_street


def parse_date(accident):
    """
    parses an accident's date
    """
    year = int(accident.get("SHNAT_TEUNA"))
    month = int(accident.get("HODESH_TEUNA"))
    day = int(accident.get("YOM_BE_HODESH"))

    """
    hours calculation explanation - The value of the hours is between 1 to 96.
    These values represent 15 minutes each that start at 00:00:
    1 equals 00:00, 2 equals 00:15, 3 equals 00:30 and so on.
    """
    minutes = accident.get("SHAA") * 15 - 15
    hours = int(minutes // 60)
    minutes %= 60
    minutes = int(minutes)
    accident_date = datetime.datetime(year, month, day, hours, minutes, 0)
    return accident_date


def get_junction(accident, roads):
    """
    extracts the junction from an accident
    omerxx: added "km" parameter to the calculation to only show the right junction,
    every non-urban accident shows nearest junction with distance and direction
    :return: returns the junction or None if it wasn't found
    """
    if (
        accident.get("KM") is not None
        and accident.get("ZOMET_LO_IRONI") is None
    ):
        min_dist = 100000
        key = (), ()
        junc_km = 0
        for option in roads:
            if (
                accident.get("KVISH1") == option[0]
                and abs(accident["KM"] - option[2]) < min_dist
            ):
                min_dist = abs(accident.get("KM") - option[2])
                key = accident.get("KVISH1"), option[1], option[2]
                junc_km = option[2]
        junction = roads.get(key, None)
        if junction:
            if accident.get("KM") - junc_km > 0:
                direction = "צפונית" if accident.get("KVISH1") % 2 == 0 else "מזרחית"
            else:
                direction = "דרומית" if accident.get("KVISH1") % 2 == 0 else "מערבית"
            if abs(float(accident["KM"] - junc_km) / 10) >= 1:
                string = (
                    str(abs(float(accident["KM"]) - junc_km) / 10)
                    + " ק״מ "
                    + direction
                    + " ל"
                    + junction
                )
            elif 0 < abs(float(accident["KM"] - junc_km) / 10) < 1:
                string = (
                    str(int((abs(float(accident.get("KM")) - junc_km) / 10) * 1000))
                    + " מטרים "
                    + direction
                    + " ל"
                    + junction
                )
            else:
                string = junction
            return string
        else:
            return ""

    elif accident.get("ZOMET_LO_IRONI") is not None:
        key = (
            accident.get("KVISH1"),
            accident.get("KVISH2"),
            accident.get("KM"),
        )
        junction = roads.get(key, None)
        return junction if junction else ""
    else:
        return ""


def load_extra_data(accident, streets, roads):
    """
    loads more data about the accident
    :return: a dictionary containing all the extra fields and their values
    :rtype: dict
    """
    extra_fields = {}
    # if the accident occurred in an urban setting
    if bool(accident.get("ZOMET_IRONI")):
        main_street, secondary_street = get_streets(accident, streets)
        if main_street:
            extra_fields["REHOV1"] = main_street
        if secondary_street:
            extra_fields["REHOV2"] = secondary_street

    # if the accident occurred in a non urban setting (highway, etc')
    if bool(accident.get("ZOMET_LO_IRONI")):
        junction = get_junction(accident, roads)
        if junction:
            extra_fields["SHEM_ZOMET"] = junction

    # localize static accident values
    for field in get_supported_localization_tables():
        # if we have a localized field for that particular field, save the field value
        # it will be fetched we deserialized
        if accident.get(field) and get_localization_field(field, accident.get(field)):
            extra_fields[field] = accident.get(field)

    return extra_fields


def get_non_urban_intersection(accident, roads):
    """
    extracts the non-urban-intersection from an accident
    """
    non_urban_intersection_value = accident.get("ZOMET_LO_IRONI")
    if non_urban_intersection_value is not None and not math.isnan(non_urban_intersection_value):
        road1 = accident.get("KVISH1")
        road2 = accident.get("KVISH2")
        km = accident.get("KM")
        key = (road1, road2, km)
        junction = roads.get(key, None)
        if junction is None:
            road2 = 0 if road2 is None or math.isnan(road2) else road2
            km = 0 if km is None or math.isnan(km) else km
            if road2 == 0 or km == 0:
                key = (road1, road2, km)
                junction = roads.get(key, None)
        return junction
    return None


def get_non_urban_intersection_by_junction_number(accident, non_urban_intersection):
    non_urban_intersection_value = accident.get("ZOMET_LO_IRONI")
    if non_urban_intersection_value is not None and not math.isnan(non_urban_intersection_value):
        key = accident.get("ZOMET_LO_IRONI")
        junction = non_urban_intersection.get(key, None)
        return junction


def get_marker(accident, streets, roads, non_urban_intersection):
    if "X" not in accident or "Y" not in accident:
        raise ValueError("Missing x and y coordinates")
    if (
        accident.get("X")
        and not math.isnan(accident.get("X"))
        and accident.get("Y")
        and not math.isnan(accident.get("Y"))
    ):
        lng, lat = coordinates_converter.convert(
            accident.get("X"), accident.get("Y")
        )
    else:
        lng, lat = None, None  # Must insert everything to avoid foreign key failure
    main_street, secondary_street = get_streets(accident, streets)
    km = accident.get("KM")
    km = None if km is None or math.isnan(km) else str(km)
    km_accurate = None
    if km is not None:
        km_accurate = False if "-" in km else True
        km = float(km.strip("-"))
    accident_datetime = parse_date(accident)
    marker = {
        "id": int(accident.get("PK_TEUNA_FIKT")),
        "provider_and_id": int(
            str(int(accident.get("SUG_TIK"))) + str(int(accident.get("PK_TEUNA_FIKT")))
        ),
        "provider_code": int(accident.get("SUG_TIK")),
        "file_type_police": get_data_value(accident.get("SUG_TIK_MISHTARA")),
        "title": "Accident",
        "description": json.dumps(load_extra_data(accident, streets, roads)),
        "address": get_address(accident, streets),
        "latitude": lat,
        "longitude": lng,
        "accident_type": get_data_value(accident.get("SUG_TEUNA")),
        "accident_severity": get_data_value(accident.get("HUMRAT_TEUNA")),
        "created": accident_datetime,
        "location_accuracy": get_data_value(accident.get("STATUS_IGUN")),
        "road_type": get_data_value(accident.get("SUG_DEREH")),
        "road_shape": get_data_value(accident.get("ZURAT_DEREH")),
        "day_type": get_data_value(accident.get("SUG_YOM")),
        "police_unit": get_data_value(accident.get("YEHIDA")),
        "mainStreet": main_street,
        "secondaryStreet": secondary_street,
        "junction": get_junction(accident, roads),
        "one_lane": get_data_value(accident.get("HAD_MASLUL")),
        "multi_lane": get_data_value(accident.get("RAV_MASLUL")),
        "speed_limit": get_data_value(accident.get("MEHIRUT_MUTERET")),
        "road_intactness": get_data_value(accident.get("TKINUT")),
        "road_width": get_data_value(accident.get("ROHAV")),
        "road_sign": get_data_value(accident.get("SIMUN_TIMRUR")),
        "road_light": get_data_value(accident.get("TEURA")),
        "road_control": get_data_value(accident.get("BAKARA")),
        "weather": get_data_value(accident.get("MEZEG_AVIR")),
        "road_surface": get_data_value(accident.get("PNE_KVISH")),
        "road_object": get_data_value(accident.get("SUG_EZEM")),
        "object_distance": get_data_value(accident.get("MERHAK_EZEM")),
        "didnt_cross": get_data_value(accident.get("LO_HAZA")),
        "cross_mode": get_data_value(accident.get("OFEN_HAZIYA")),
        "cross_location": get_data_value(accident.get("MEKOM_HAZIYA")),
        "cross_direction": get_data_value(accident.get("KIVUN_HAZIYA")),
        "road1": get_data_value(accident.get("KVISH1")),
        "road2": get_data_value(accident.get("KVISH2")),
        "km": km,
        "km_raw": get_data_value(accident.get("KM")),
        "km_accurate": km_accurate,
        "yishuv_symbol": get_data_value(accident.get("SEMEL_YISHUV")),
        "yishuv_name": get_localization_city_name(accident.get("SEMEL_YISHUV")),
        "geo_area": get_data_value(accident.get("THUM_GEOGRAFI")),
        "day_night": get_data_value(accident.get("YOM_LAYLA")),
        "day_in_week": get_data_value(accident.get("YOM_BASHAVUA")),
        "traffic_light": get_data_value(accident.get("RAMZOR")),
        "region": get_data_value(accident.get("MAHOZ")),
        "district": get_data_value(accident.get("NAFA")),
        "natural_area": get_data_value(accident.get("EZOR_TIVI")),
        "municipal_status": get_data_value(accident.get("MAAMAD_MINIZIPALI")),
        "yishuv_shape": get_data_value(accident.get("ZURAT_ISHUV")),
        "street1": get_data_value(accident.get("REHOV1")),
        "street1_hebrew": get_street(
            accident.get("SEMEL_YISHUV"), accident.get("REHOV1"), streets
        ),
        "street2": get_data_value(accident.get("REHOV2")),
        "street2_hebrew": get_street(
            accident.get("SEMEL_YISHUV"), accident.get("REHOV2"), streets
        ),
        "house_number": get_data_value(accident.get("BAYIT")),
        "urban_intersection": get_data_value(accident.get("ZOMET_IRONI")),
        "non_urban_intersection": get_data_value(accident.get("ZOMET_LO_IRONI")),
        "non_urban_intersection_hebrew": get_non_urban_intersection(accident, roads),
        "non_urban_intersection_by_junction_number": get_non_urban_intersection_by_junction_number(
            accident, non_urban_intersection
        ),
        "accident_year": get_data_value(accident.get("SHNAT_TEUNA")),
        "accident_month": get_data_value(accident.get("HODESH_TEUNA")),
        "accident_day": get_data_value(accident.get("YOM_BE_HODESH")),
        "accident_hour_raw": get_data_value(accident.get("SHAA")),
        "accident_hour": accident_datetime.hour,
        "accident_minute": accident_datetime.minute,
        "x": accident.get("X"),
        "y": accident.get("Y"),
        "vehicle_type_rsa": None,
        "violation_type_rsa": None,
        "geom": None,
    }
    return marker


def get_accidents(stats, files_from_cbs):
    for _, accident in files_from_cbs['accidents'].iterrows():
        stats['accidents'] += 1
        yield get_marker(
            accident,
            files_from_cbs['streets'],
            files_from_cbs['roads'],
            files_from_cbs['non_urban_intersection']
        )


def main(load_start_year=None):
    common_main(load_start_year, CBS_ACCIDENT_MARKERS_ROOT_PATH, get_accidents)
