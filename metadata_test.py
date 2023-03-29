import requests
import json
from logzero import logger

from yaml_to_sql import YamlReader, TYPES_MAPPING


def infer_type(col, table_info, yaml_reader):
    """Guess the type of a new column read from HHS JSON."""
    ret = {}
    sqltys = -1
    col_name = col["fieldName"]
    if col["dataTypeName"] == "text":
        if "cachedContents" not in col:
            try:
                yaml_columns = yaml_reader.get_table_ordered_csv_columns(table_info)
                yaml_column = list(filter(lambda col: col[1] == col_name or col[2] == col_name, yaml_columns))[0]
                pyty = yaml_column[0].split(":")[0] if ":" in yaml_column[0] else yaml_column[0]
                sqlty = TYPES_MAPPING[pyty]
                return {"name": col_name, "py_type": pyty, "sql_type": sqlty}
            except KeyError:
                logger.error(f"Can't infer type and/or length without cachedContents:\n{json.dumps(col, indent=2)}")
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
    ret = {"name": col_name, "py_type": pyty, "sql_type": sqlty}
    if sqltys > 0:
        ret["sql_type_size"] = sqltys
    return ret


def main():
    yaml_reader = YamlReader("covid_hosp_schemadefs.yaml")
    table_info = yaml_reader.get_table_info("covid_hosp_facility")
    yaml_columns = yaml_reader.get_table_ordered_csv_columns(table_info)

    metadata_info = requests.get("https://healthdata.gov/api/views/anag-cw7u.json").json()
    metadata_columns = list(map(lambda col: infer_type(col, table_info, yaml_reader), metadata_info["columns"]))

    # print(yaml_columns)
    print(metadata_columns)


if __name__ == "__main__":
    main()
