import os
from collections import defaultdict

import pandas as pd


ACCIDENTS = "accidents"
CITIES = "cities"
STREETS = "streets"
ROADS = "roads"
URBAN_INTERSECTION = "urban_intersection"
NON_URBAN_INTERSECTION = "non_urban_intersection"
DICTIONARY = "dictionary"
INVOLVED = "involved"
VEHICLES = "vehicles"

cbs_files = {
    ACCIDENTS: "AccData.csv",
    URBAN_INTERSECTION: "IntersectUrban.csv",
    NON_URBAN_INTERSECTION: "IntersectNonUrban.csv",
    STREETS: "DicStreets.csv",
    DICTIONARY: "Dictionary.csv",
    INVOLVED: "InvData.csv",
    VEHICLES: "VehData.csv",
}

DICTCOLUMN1 = "MS_TAVLA"
DICTCOLUMN2 = "KOD"
DICTCOLUMN3 = "TEUR"


def read_dictionary(dictionary_file):
    cbs_dictionary = defaultdict(dict)
    dictionary = pd.read_csv(dictionary_file, encoding="cp1255")
    for _, dic in dictionary.iterrows():
        cbs_dictionary[int(dic[DICTCOLUMN1])][int(dic[DICTCOLUMN2])] = dic[DICTCOLUMN3]
    return cbs_dictionary


def get_files(directory):
    output_files_dict = {}
    if os.path.exists(directory):
        for name, filename in cbs_files.items():
            if name not in (STREETS, NON_URBAN_INTERSECTION, ACCIDENTS, INVOLVED, VEHICLES, DICTIONARY):
                continue
            files = [path for path in os.listdir(directory) if filename.lower() in path.lower()]
            amount = len(files)
            if amount == 0:
                raise ValueError("Not found: '%s'" % filename)
            if amount > 1:
                raise ValueError("Ambiguous: '%s'" % filename)
            file_path = os.path.join(directory, files[0])
            if name == DICTIONARY:
                output_files_dict[name] = read_dictionary(file_path)
            elif name in (ACCIDENTS, INVOLVED, VEHICLES):
                df = pd.read_csv(file_path, encoding="cp1255")
                df.columns = [column.upper() for column in df.columns]
                output_files_dict[name] = df
            else:
                df = pd.read_csv(file_path, encoding="cp1255")
                df.columns = [column.upper() for column in df.columns]
                if name == STREETS:
                    streets_map = {}
                    groups = df.groupby("ISHUV")
                    for key, settlement in groups:
                        streets_map[key] = [
                            {
                                "SEMEL_RECHOV": x["SEMEL_RECHOV"],
                                "SHEM_RECHOV": x["SHEM_RECHOV"],
                            }
                            for _, x in settlement.iterrows()
                        ]

                    output_files_dict[name] = streets_map
                elif name == NON_URBAN_INTERSECTION:
                    roads = {
                        (x["KVISH1"], x["KVISH2"], x["KM"]): x[
                            "SHEM_ZOMET"
                        ]
                        for _, x in df.iterrows()
                    }
                    non_urban_intersection = {
                        x["ZOMET"]: x["SHEM_ZOMET"] for _, x in df.iterrows()
                    }
                    output_files_dict[ROADS] = roads
                    output_files_dict[NON_URBAN_INTERSECTION] = non_urban_intersection
    return output_files_dict
