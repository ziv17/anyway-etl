import os
import math
import datetime
from dateutil.relativedelta import relativedelta

import dataflows as DF
from sqlalchemy import or_

from anyway.db_views import VIEWS
from anyway.models import AccidentMarker, Involved, Vehicle, ProviderCode

from ..db import session_decorator
from .config import (
    CBS_YEARLY_DIRECTORIES_ROOT_PATH, CBS_ACCIDENT_MARKERS_ROOT_PATH,
    CBS_INVOLVED_ROOT_PATH, CBS_VEHICLES_ROOT_PATH
)
from .get_files import get_files
from .parse_common import get_saved_data


TABLES_DICT = {
    0: "columns_description",
    1: "police_unit",
    2: "road_type",
    4: "accident_severity",
    5: "accident_type",
    9: "road_shape",
    10: "one_lane",
    11: "multi_lane",
    12: "speed_limit",
    13: "road_intactness",
    14: "road_width",
    15: "road_sign",
    16: "road_light",
    17: "road_control",
    18: "weather",
    19: "road_surface",
    21: "road_object",
    22: "object_distance",
    23: "didnt_cross",
    24: "cross_mode",
    25: "cross_location",
    26: "cross_direction",
    28: "driving_directions",
    30: "vehicle_status",
    31: "involved_type",
    34: "safety_measures",
    35: "injury_severity",
    37: "day_type",
    38: "day_night",
    39: "day_in_week",
    40: "traffic_light",
    43: "vehicle_attribution",
    45: "vehicle_type",
    50: "injured_type",
    52: "injured_position",
    60: "accident_month",
    66: "population_type",
    67: "sex",
    68: "geo_area",
    77: "region",
    78: "municipal_status",
    79: "district",
    80: "natural_area",
    81: "yishuv_shape",
    92: "age_group",
    93: "accident_hour_raw",
    111: "engine_volume",
    112: "total_weight",
    200: "hospital_time",
    201: "medical_type",
    202: "release_dest",
    203: "safety_measures_use",
    204: "late_deceased",
    205: "location_accuracy",
    229: "vehicle_damage",
    245: "vehicle_type",
}


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


def time_delta(since):
    delta = relativedelta(datetime.datetime.now(), since)
    attrs = ["years", "months", "days", "hours", "minutes", "seconds"]
    return " ".join(
        "%d %s" % (getattr(delta, attr), getattr(delta, attr) > 1 and attr or attr[:-1])
        for attr in attrs
        if getattr(delta, attr)
    )


@session_decorator
def delete_cbs_entries(session, start_year, batch_size=5000):
    """
    deletes all CBS markers (provider_code=1 or provider_code=3) in the database created in year and with provider code provider_code
    first deletes from tables Involved and Vehicle, then from table AccidentMarker
    """
    print("Deleting old cbs entries (start_year={} batch_size={})".format(start_year, batch_size))
    start_date = f"{start_year}-01-01"
    marker_ids_to_delete = (
        session.query(AccidentMarker.id)
        .filter(AccidentMarker.created >= datetime.datetime.strptime(start_date, "%Y-%m-%d"))
        .filter(
            or_(
                (AccidentMarker.provider_code == 1),
                (AccidentMarker.provider_code == 3),
            )
        )
        .all()
    )
    marker_ids_to_delete = [acc_id[0] for acc_id in marker_ids_to_delete]
    print(
        "There are "
        + str(len(marker_ids_to_delete))
        + " accident ids to delete starting "
        + str(start_date)
    )
    for ids_chunk in chunks(marker_ids_to_delete, batch_size):
        print("Deleting a chunk of " + str(len(ids_chunk)))
        q = session.query(Involved).filter(Involved.accident_id.in_(ids_chunk))
        if q.all():
            print("deleting entries from Involved")
            q.delete(synchronize_session=False)
            session.commit()

        q = session.query(Vehicle).filter(Vehicle.accident_id.in_(ids_chunk))
        if q.all():
            print("deleting entries from Vehicle")
            q.delete(synchronize_session=False)
            session.commit()

        q = session.query(AccidentMarker).filter(AccidentMarker.id.in_(ids_chunk))
        if q.all():
            print("deleting entries from AccidentMarker")
            q.delete(synchronize_session=False)
            session.commit()
    session.commit()


@session_decorator
def create_provider_code_table(session):
    provider_code_table = "provider_code"
    provider_code_class = ProviderCode
    table_entries = session.query(provider_code_class)
    table_entries.delete()
    provider_code_dict = {
        1: "הלשכה המרכזית לסטטיסטיקה - סוג תיק 1",
        2: "איחוד הצלה",
        3: "הלשכה המרכזית לסטטיסטיקה - סוג תיק 3",
        4: "שומרי הדרך",
    }
    for k, v in provider_code_dict.items():
        sql_insert = (
            "INSERT INTO " + provider_code_table + " VALUES (" + str(k) + "," + "'" + v + "'" + ")"
        )
        session.execute(sql_insert)
        session.commit()


@session_decorator
def fill_dictionary_tables(session, cbs_dictionary, provider_code, year):
    if year < 2008:
        return
    for k, v in cbs_dictionary.items():
        if k == 97:
            continue
        try:
            curr_table = TABLES_DICT[k]
        except Exception as _:
            print(
                "A key " + str(k) + " was added to dictionary - update models, tables and classes"
            )
            continue
        for inner_k, inner_v in v.items():
            if inner_v is None or (isinstance(inner_v, float) and math.isnan(inner_v)):
                continue
            sql_delete = (
                "DELETE FROM "
                + curr_table
                + " WHERE provider_code="
                + str(provider_code)
                + " AND year="
                + str(year)
                + " AND id="
                + str(inner_k)
            )
            session.execute(sql_delete)
            session.commit()
            sql_insert = (
                "INSERT INTO "
                + curr_table
                + " VALUES ("
                + str(inner_k)
                + ","
                + str(year)
                + ","
                + str(provider_code)
                + ","
                + "'"
                + inner_v.replace("'", "")
                + "'"
                + ")"
                + " ON CONFLICT DO NOTHING"
            )
            session.execute(sql_insert)
            session.commit()
        print("Inserted/Updated dictionary values into table " + curr_table)



@session_decorator
def fill_db_geo_data(session):
    """
    Fills empty geometry object according to coordinates in database
    SRID = 4326
    """
    session.execute(
        "UPDATE markers SET geom = ST_SetSRID(ST_MakePoint(longitude,latitude),4326)\
                           WHERE geom IS NULL;"
    )
    session.commit()


@session_decorator
def import_accidents(session, provider_code, year):
    print("Importing markers")
    accidents_result = get_saved_data(os.path.join(CBS_ACCIDENT_MARKERS_ROOT_PATH, 'datapackage.json'), provider_code, year)
    session.bulk_insert_mappings(AccidentMarker, accidents_result)
    session.commit()
    print("Finished Importing markers")
    print("Inserted " + str(len(accidents_result)) + " new accident markers")
    fill_db_geo_data()
    return len(accidents_result)


@session_decorator
def import_involved(session, provider_code, year):
    print("Importing involved")
    involved_result = get_saved_data(os.path.join(CBS_INVOLVED_ROOT_PATH, 'datapackage.json'), provider_code, year)
    session.bulk_insert_mappings(Involved, involved_result)
    session.commit()
    print("Finished Importing involved")
    return len(involved_result)


@session_decorator
def import_vehicles(session, provider_code, year):
    print("Importing vehicles")
    vehicles_result = get_saved_data(os.path.join(CBS_VEHICLES_ROOT_PATH, 'datapackage.json'), provider_code, year)
    print("Finished Importing vehicles")
    session.bulk_insert_mappings(Vehicle, vehicles_result)
    return len(vehicles_result)


def import_provider_year_to_datastore(directory, provider_code, year, failed_dirs, batch_size=5000):
    """
    goes through all the files in a given directory, parses and commits them
    """
    try:
        files_from_cbs = get_files(directory)
        if len(files_from_cbs) == 0:
            return 0
        print("Importing '{}'".format(directory))
        started = datetime.datetime.now()
        # import dictionary
        fill_dictionary_tables(files_from_cbs['dictionary'], provider_code, year)
        new_items = 0
        accidents_count = import_accidents(provider_code, year)
        new_items += accidents_count
        involved_count = import_involved(provider_code, year)
        new_items += involved_count
        vehicles_count = import_vehicles(provider_code, year)
        new_items += vehicles_count
        print("\t{0} items in {1}".format(new_items, time_delta(started)))
        return new_items
    except ValueError as e:
        failed_dirs[directory] = str(e)
        if "Not found" in str(e):
            return 0
        raise (e)


@session_decorator
def create_tables(session):
    session.execute("TRUNCATE involved_markers_hebrew")
    session.execute("TRUNCATE vehicles_markers_hebrew")
    session.execute("TRUNCATE vehicles_hebrew")
    session.execute("TRUNCATE involved_hebrew")
    session.execute("TRUNCATE markers_hebrew")
    session.execute("INSERT INTO markers_hebrew " + VIEWS.MARKERS_HEBREW_VIEW)
    session.execute("INSERT INTO involved_hebrew " + VIEWS.INVOLVED_HEBREW_VIEW)
    session.execute("INSERT INTO vehicles_hebrew " + VIEWS.VEHICLES_HEBREW_VIEW)
    session.execute("INSERT INTO vehicles_markers_hebrew " + VIEWS.VEHICLES_MARKERS_HEBREW_VIEW)
    session.execute(
        "INSERT INTO involved_markers_hebrew " + VIEWS.INVOLVED_HEBREW_MARKERS_HEBREW_VIEW
    )
    session.commit()
    print("Created DB Hebrew Tables")


def main(load_start_year=None):
    started = datetime.datetime.now()
    load_start_year = int(load_start_year) if load_start_year else datetime.datetime.now().year - 1
    delete_cbs_entries(load_start_year)
    create_provider_code_table()
    current_year = datetime.datetime.now().year
    total = 0
    failed_dirs = {}
    for provider_code in [1, 3]:
        for year in range(load_start_year, current_year + 1):
            cbs_files_dir = os.path.join(
                CBS_YEARLY_DIRECTORIES_ROOT_PATH,
                'accidents_type_{}'.format(provider_code),
                str(year)
            )
            total += import_provider_year_to_datastore(cbs_files_dir, provider_code, year, failed_dirs)
    fill_db_geo_data()
    failed = [
        "\t'{0}' ({1})".format(directory, fail_reason)
        for directory, fail_reason in failed_dirs.items()
    ]
    print(
        "Finished processing all directories{0}{1}".format(
            ", except:\n" if failed else "", "\n".join(failed)
        )
    )
    print("Total: {0} items in {1}".format(total, time_delta(started)))
    create_tables()
