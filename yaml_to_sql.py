import yaml


types_mapping = {
    "int": "INT(11)",
    "float": "DOUBLE",
    "str": "VARCHAR(255)",
    "intdate": "INT(11)",
    "geocode": "VARCHAR(32)",
    "bool": "TINYINT(1)"
}

newline = "\n\t"


def yaml_to_json(filename, tablename):
    with open(f"{filename}.yaml", "r") as stream:
        try:
            return yaml.safe_load(stream)[tablename]
        except yaml.YAMLError as exc:
            print(exc)


def create_sql_statement(yaml_representation):
    sql_statement = f"""
    CREATE TABLE `{yaml_representation["TABLE_NAME"]}` (
        `id` INT NOT NULL AUTO_INCREMENT,
        `issue` INT NOT NULL,
        {f",{newline}".join([f"`{column[1]}` {types_mapping[column[2]]} {'NOT NULL' if column[1] in yaml_representation['KEY_COLS'] else ''}".strip() for column in yaml_representation["ORDERED_CSV_COLUMNS"]])}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """
    return sql_statement


def main():
    table_name = "covid_hosp_facility"
    table_info = yaml_to_json("covid_hosp_schemadefs", table_name)
    statement = create_sql_statement(table_info)
    print(statement)


if __name__ == "__main__":
    main()
