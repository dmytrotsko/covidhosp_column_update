import yaml

TYPES_MAPPING = {
    "int": "INT(11)",
    "float": "DOUBLE",
    "str": "VARCHAR(255)",
    "intdate": "INT(11)",
    "geocode": "VARCHAR(32)",
    "bool": "TINYINT(1)",
}

NEWLINE = "\n\t"


class YamlReader:
    def __init__(self, schemadef_yaml_path):
        self.schemadef_yaml_path = schemadef_yaml_path
        self.tables_info = self.yaml_to_json()

    def yaml_to_json(self):
        with open(self.schemadef_yaml_path, "r") as f:
            try:
                return yaml.safe_load(f)
            except yaml.YAMLError as exc:
                print(exc)

    def get_table_info(self, entry_name):
        table_info = self.tables_info.get(entry_name)
        return table_info

    def get_table_key_columns(self, table_info):
        key_columns = table_info["KEY_COLS"]
        return key_columns

    def get_table_ordered_csv_columns(self, table_info):
        ordered_csv_columns = table_info["ORDERED_CSV_COLUMNS"]
        return ordered_csv_columns

    def get_table_unique_indexes(self, table_info):
        unique_indexes = table_info["UNIQUE_INDEXES"]
        return unique_indexes

    def get_table_indexes(self, table_info):
        indexes = table_info["INDEXES"]
        return indexes

    def get_table_name(self, table_info):
        table_name = table_info["TABLE_NAME"]
        return table_name

    def generate_create_table_statement(self, entry_name):
        table_info = self.get_table_info(entry_name)
        columns = []
        for column in self.get_table_ordered_csv_columns(table_info):
            column_name = column[1] if column[2] is None else column[2]
            column_type = TYPES_MAPPING.get(column[0])
            not_null = "NOT NULL" if column_name in self.get_table_key_columns(table_info) else ""
            columns.append(f"`{column_name}` {column_type} {not_null}".strip())

        unique_keys = []
        for k, v in table_info["UNIQUE_INDEXES"].items():
            cols = ", ".join([f"`{col}`" for col in v])
            unique_keys.append(f"UNIQUE KEY `{k}` ({cols})")

        keys = []
        for k, v in table_info["INDEXES"].items():
            cols = ", ".join([f"`{col}`" for col in v])
            keys.append(f"KEY `{k}` ({cols})")

        sql_statement = f"""
        CREATE TABLE `{self.get_table_name(table_info)}` (
        `id` INT NOT NULL AUTO_INCREMENT,
        `issue` INT NOT NULL,
        {f",{NEWLINE}".join([column for column in columns])},
        PRIMARY KEY (`id`),
        {f",{NEWLINE}".join([u for u in unique_keys])},
        {f",{NEWLINE}".join([k for k in keys])}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
        return sql_statement

    def generate_create_metadata_table_statement(self, entry_name):
        table_info = self.get_table_info(entry_name)
        aggregated_key_cols = [
            column
            for column in table_info["ORDERED_CSV_COLUMNS"]
            if column[1] in table_info["AGGREGATE_KEY_COLS"]
        ]
        columns = []
        for column in aggregated_key_cols:
            column_name = column[1] if column[2] is None else column[2]
            column_type = TYPES_MAPPING.get(column[0])
            not_null = "NOT NULL" if column_name in self.get_table_key_columns(table_info) else ""
            columns.append(f"`{column_name}` {column_type} {not_null}".strip())

        unique_keys = []
        for k, v in table_info["UNIQUE_INDEXES"].items():
            if k not in table_info["AGGREGATE_KEY_COLS"]:
                continue
            cols = []
            for col in v:
                if col in table_info["AGGREGATE_KEY_COLS"]:
                    cols.append(col)
            cols = ", ".join([f"`{col}`" for col in cols])
            unique_keys.append(f"UNIQUE KEY `{k}` ({cols})")

        keys = []
        for k, v in table_info["INDEXES"].items():
            if k not in table_info["AGGREGATE_KEY_COLS"]:
                continue
            cols = []
            for col in v:
                if col in table_info["AGGREGATE_KEY_COLS"]:
                    cols.append(col)
            cols = ", ".join([f"`{col}`" for col in cols])
            keys.append(f"KEY `{k}` ({cols})")

        sql_statement = f"""
        CREATE TABLE `{self.get_table_name(table_info)}` (
        `id` INT NOT NULL AUTO_INCREMENT,
        {f",{NEWLINE}".join([column for column in columns])},
        PRIMARY KEY (`id`),
        {f",{NEWLINE}".join([u for u in unique_keys])},
        {f",{NEWLINE}".join([k for k in keys])}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """

        return sql_statement


def main():
    yaml_reader = YamlReader("covid_hosp_schemadefs.yaml")
    ddl = yaml_reader.generate_create_table_statement("covid_hosp_facility")
    print(ddl)
    metadata = yaml_reader.generate_create_metadata_table_statement("covid_hosp_facility")
    print("*" * 20)
    print(metadata)


# TODO: generate covid_hosp_facility_key

if __name__ == "__main__":
    main()
