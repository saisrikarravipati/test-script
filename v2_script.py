class DataInserter:
    """
    Class responsible for inserting and updating data into the database using InsightsDB.
    """

    def __init__(self):
        """
        Initialize the DataInserter with an instance of InsightsDB.
        """
        # Initialize the database connection
        self.db = insightsdb()  # Ensure 'insightsdb' is correctly initialized

        # Define table configurations
        self.table_configs = {
            "unit_test_records": {
                "unique_keys": ["artifact_url", "artifact_name", "artifact_version"],
                "excluded_columns": ["id", "created_date"],
                "json_columns": ["artifact_info", "message_blob", "policy_reasons"]
            },
            "sevenps_result_set": {
                "unique_keys": ["test_request_id"],
                "excluded_columns": ["id", "created_date"],
                "json_columns": ["report_doc", "traceability_doc"]
            },
            "jira_issues": {
                "unique_keys": ["jira_key", "repo_url"],
                "excluded_columns": ["id", "created_date"],
                "json_columns": ["issue_data"]
            },
            "manual_test_records": {
                "unique_keys": ["jira_key", "artifact_url"],
                "excluded_columns": ["id", "created_date"],
                "json_columns": ["execution_result"]
            }
        }

        # Initialize counters
        self.total_records = 0
        self.total_insertions = 0
        self.total_updates = 0

    def serialize_field(self, value: Any) -> str:
        """
        Serialize a field to JSON if it's a dict or list, else return as string.

        :param value: The value to serialize.
        :return: Serialized JSON string or string representation of the value.
        """
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        elif value is None:
            return ""
        else:
            return str(value)

    def records_differ(self, existing_record: Dict[str, Any], new_record: Dict[str, Any], json_columns: List[str]) -> bool:
        """
        Compare JSON fields between existing and new records to determine if they differ.

        :param existing_record: The existing record from the database as a dictionary.
        :param new_record: The new record to compare.
        :param json_columns: List of columns that contain JSON data.
        :return: True if any JSON field differs, False otherwise.
        """
        for column in json_columns:
            existing_value = existing_record.get(column, "")
            new_value = self.serialize_field(new_record.get(column, ""))
            if existing_value != new_value:
                logger.debug(f"Difference found in column '{column}': existing='{existing_value}' vs new='{new_value}'")
                return True
        return False

    def insert_or_update_record(self, table_name: str, record: Dict[str, Any]):
        """
        Insert a new record or update an existing record in the specified table.

        :param table_name: Name of the table.
        :param record: Dictionary containing column-value pairs.
        """
        self.total_records += 1  # Increment total records counter
        config = self.table_configs.get(table_name)
        if not config:
            logger.warning(f"No configuration found for table '{table_name}'. Skipping record.")
            return

        unique_keys = config["unique_keys"]
        excluded_columns = config["excluded_columns"]
        json_columns = config.get("json_columns", [])

        # Extract unique key values
        try:
            unique_values = {key: record[key] for key in unique_keys}
            logger.debug(f"Unique keys for table '{table_name}': {unique_values}")
        except KeyError as ke:
            logger.error(f"Missing unique key '{ke.args[0]}' for table '{table_name}'. Record: {record}. Skipping.")
            return

        # Construct WHERE clause for unique keys
        where_clause = " AND ".join([f"{key} = %s" for key in unique_keys])
        where_values = tuple(unique_values[key] for key in unique_keys)

        # Check if record exists
        select_query = f"SELECT * FROM {table_name} WHERE {where_clause} LIMIT 1;"
        try:
            logger.debug(f"Executing SELECT query: {select_query} with values {where_values}")
            result = self.db._execute(select_query, where_values)
        except Exception as e:
            logger.error(f"Error executing SELECT for table '{table_name}': {e}")
            return

        if result:
            # Assuming db._execute returns a list of dictionaries
            existing_record = result[0]
            logger.debug(f"Existing record found in '{table_name}': {existing_record}")
            # Check if JSON fields differ
            if self.records_differ(existing_record, record, json_columns):
                # Prepare fields for update
                update_fields = []
                update_values = []
                for column, value in record.items():
                    if column in excluded_columns:
                        continue  # Skip excluded columns
                    if column in json_columns:
                        serialized = self.serialize_field(value)
                        update_fields.append(f"{column} = %s")
                        update_values.append(serialized)
                    else:
                        update_fields.append(f"{column} = %s")
                        update_values.append(value)

                # Append unique key values for WHERE clause
                update_values.extend([unique_values[key] for key in unique_keys])

                update_query = f"UPDATE {table_name} SET {', '.join(update_fields)} WHERE {where_clause};"
                try:
                    logger.debug(f"Executing UPDATE query: {update_query} with values {update_values}")
                    self.db._execute(update_query, tuple(update_values))
                    logger.info(f"Updated record in '{table_name}': {unique_values}")
                    self.total_updates += 1  # Increment updates counter
                except Exception as e:
                    logger.error(f"Error updating record in '{table_name}': {e}")
            else:
                logger.info(f"No changes detected for record in '{table_name}': {unique_values}. Skipping update.")
        else:
            # Prepare fields for insertion
            insert_fields = []
            insert_placeholders = []
            insert_values = []
            for column, value in record.items():
                if column in excluded_columns:
                    continue  # Skip excluded columns
                insert_fields.append(column)
                insert_placeholders.append("%s")
                if column in json_columns:
                    serialized = self.serialize_field(value)
                    insert_values.append(serialized)
                else:
                    insert_values.append(value)

            insert_query = f"INSERT INTO {table_name} ({', '.join(insert_fields)}) VALUES ({', '.join(insert_placeholders)});"
            try:
                logger.debug(f"Executing INSERT query: {insert_query} with values {insert_values}")
                self.db._execute(insert_query, tuple(insert_values))
                logger.info(f"Inserted new record into '{table_name}': {unique_values}")
                self.total_insertions += 1  # Increment insertions counter
            except Exception as e:
                logger.error(f"Error inserting record into '{table_name}': {e}")

    def insert_data(self, data: Dict[str, List[Dict[str, Any]]]):
        """
        Insert or update records into their respective tables.

        :param data: Dictionary containing table names as keys and lists of records as values.
        """
        for table_name, records in data.items():
            if not isinstance(records, list):
                logger.warning(f"Data for table '{table_name}' is not a list. Skipping.")
                continue

            logger.info(f"Processing {len(records)} records for table '{table_name}'.")
            for record in records:
                if not isinstance(record, dict):
                    logger.warning(f"Record in table '{table_name}' is not a dictionary. Skipping.")
                    continue

                self.insert_or_update_record(table_name, record)

        # After processing all records, log the statistics
        logger.info("----- Data Insertion Statistics -----")
        logger.info(f"Total Records Processed: {self.total_records}")
        logger.info(f"Total Insertions: {self.total_insertions}")
        logger.info(f"Total Updates: {self.total_updates}")
        logger.info("-------------------------------------")
