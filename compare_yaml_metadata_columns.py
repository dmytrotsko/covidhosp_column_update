import json

import requests
from logzero import logger

from yaml_to_sql import YamlReader

MAX_SQL_NAME_LENGTH = 64


#    state ts: "https://healthdata.gov/api/views/g62h-syeh.json"
# state daily: "https://healthdata.gov/api/views/6xf2-c3ie.json"
#    facility: "https://healthdata.gov/api/views/anag-cw7u.json"


def infer_type(col):
    """Guess the type of a new column read from HHS JSON."""
    sqltys = -1
    if col["dataTypeName"] == "text":
        if "cachedContents" not in col:
            raise Exception(f"Can't infer type and/or length without cachedContents:\n{json.dumps(col, indent=2)}")
        if col["cachedContents"]["cardinality"] == "2":
            pyty = "Utils.parse_bool"
            sqlty = "BOOLEAN"
        else:
            pyty = "str"
            max_len = len(col["cachedContents"]["largest"])
            min_len = len(col["cachedContents"]["smallest"])
            if max_len - min_len == 0:
                sqlty = "CHAR"
                sqltys = max_len
            else:
                sqlty = "VARCHAR"
                sqltys = max_len + 5
    if col["dataTypeName"] == "calendar_date":
        pyty = "Utils.int_from_date"
        sqlty = "INT"
    if col["dataTypeName"] == "number":
        if ("cachedContents" in col and col["cachedContents"]["largest"].find(".") < 0) or (
            "cachedContents" not in col and any(col["name"].endswith(x) for x in ["sum", "coverage"])
        ):
            pyty = "int"
            sqlty = "INT"
        else:
            pyty = "float"
            sqlty = "DOUBLE"
    if col["dataTypeName"] == "point":
        pyty = "str"
        sqlty = "VARCHAR"
        sqltys = 32
    if col["dataTypeName"] == "checkbox":
        pyty = "Utils.parse_bool"
        sqlty = "BOOLEAN"
    ret = {"py_type": pyty, "sql_type": sqlty}
    if sqltys > 0:
        ret["sql_type_size"] = sqltys
    return ret


# ALWAYS APPLIED:
# - --> _
# + --> plus

# USUALLY APPLIED:
# _and_ --> _
# hospitalized --> hosp

# SOMETIMES APPLIED:
# vaccinated --> vaccd
# coverage --> cov
# _7_day --> 7d


def try_make_shorter(col_name):
    new_name = col_name
    strategies = [
        lambda col_name: col_name.replace("_and_", "_"),
        lambda col_name: col_name.replace("hospitalized", "hosp"),
        lambda col_name: col_name.replace("vaccinated", "vaccd"),
        lambda col_name: col_name.replace("7_day", "7d"),
        lambda col_name: col_name.replace("coverage", "cov"),
    ]
    while len(new_name) > MAX_SQL_NAME_LENGTH:
        if not strategies:
            raise Exception(f"Couldn't shorten '{new_name}' using known strategies")
        strat = strategies.pop(0)
        new_name = strat(new_name)
    return new_name


def get_sql_col_name(col_name):
    new_col_name = col_name
    strategies = [lambda col_name: col_name.replace("-", "_"), lambda col_name: col_name.replace("+", "plus")]
    for strat in strategies:
        new_col_name = strat(new_col_name)
    if len(new_col_name) > MAX_SQL_NAME_LENGTH:
        new_col_name = try_make_shorter(new_col_name)
    return new_col_name


def main():
    yaml_reader = YamlReader("covid_hosp_schemadefs.yaml")
    table_info = yaml_reader.get_table_info("state_daily")
    yaml_columns = yaml_reader.get_table_ordered_csv_columns(table_info)

    metadata_info = requests.get("https://healthdata.gov/api/views/6xf2-c3ie.json").json()
    metadata_columns = [column for column in metadata_info["columns"] if column.get("computationStrategy") is None]

    yaml_column_names = [col[1] for col in yaml_columns]
    metadata_column_names = [col["name"] for col in metadata_columns]

    missing_column_names = list(set(metadata_column_names) - set(yaml_column_names))

    missing_columns_metadata = list(filter(lambda col: col["name"] in missing_column_names, metadata_columns))

    new_columns = []
    for col in missing_columns_metadata:
        column = {"py_name": col["name"], "sql_name": get_sql_col_name(col["name"])}
        try:
            column_info = infer_type(col)
            column.update(column_info)
            new_columns.append(column)
        except Exception as e:
            logger.error(e)
            column.update({"py_type": None, "sql_type": None})
            new_columns.append(column)

    print(new_columns)
    print(len(new_columns))


if __name__ == "__main__":
    main()
