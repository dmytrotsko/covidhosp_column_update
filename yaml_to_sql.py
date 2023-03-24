import yaml


TYPES_MAPPING = {
    "int": "INT(11)",
    "float": "DOUBLE",
    "str": "VARCHAR(255)",
    "intdate": "INT(11)",
    "geocode": "VARCHAR(32)",
    "bool": "TINYINT(1)"
}

NEWLINE = "\n\t"


def yaml_to_json(filename, tablename):
    with open(f"{filename}.yaml", "r") as stream:
        try:
            return yaml.safe_load(stream)[tablename]
        except yaml.YAMLError as exc:
            print(exc)


def create_table_statement(yaml_representation):
    columns = []
    for column in yaml_representation["ORDERED_CSV_COLUMNS"]:
        if column[1] in yaml_representation["KEY_COLS"]:
            columns.append(f"`{column[1]}` {TYPES_MAPPING[column[2]]} NOT NULL")
        else:
            columns.append(f"`{column[1]}` {TYPES_MAPPING[column[2]]}")

    sql_statement = f"""
    CREATE TABLE `{yaml_representation["TABLE_NAME"]}` (
        `id` INT NOT NULL AUTO_INCREMENT,
        `issue` INT NOT NULL,
        {f",{NEWLINE}".join([column for column in columns])},
        PRIMARY KEY (`id`),
        UNIQUE KEY ({", ".join([f"`{column}`" for column in yaml_representation['KEY_COLS']])}),
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """
    return sql_statement

#####################################
def create_metadata_statement(yaml_representation):
    aggregated_key_cols = [column for column in yaml_representation["ORDERED_CSV_COLUMNS"] if column[1] in yaml_representation["AGGREGATE_KEY_COLS"]]
    columns = []
    for column in aggregated_key_cols:
        if column[1] in yaml_representation["KEY_COLS"]:
            columns.append(f"`{column[1]}` {TYPES_MAPPING[column[2]]} NOT NULL")
        else:
            columns.append(f"`{column[1]}` {TYPES_MAPPING[column[2]]}")
    sql_statement = f"""
    CREATE TABLE `{yaml_representation["TABLE_NAME"]}` (
        `id` INT NOT NULL AUTO_INCREMENT,
        {f",{NEWLINE}".join([column for column in columns])},
        PRIMARY KEY (`id`),
        UNIQUE KEY ({", ".join([f"`{column}`" for column in yaml_representation['KEY_COLS']])}),
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """
    return sql_statement


def main():
    table_name = "covid_hosp_facility"
    table_info = yaml_to_json("covid_hosp_schemadefs", table_name)
    create_table_ddl = create_table_statement(table_info)
    create_medatata_ddl = create_metadata_statement(table_info)
    print(create_table_ddl)
    print(create_medatata_ddl)


# TODO: generate covid_hosp_facility_key

if __name__ == "__main__":
    main()
