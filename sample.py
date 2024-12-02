#!/usr/bin/env python3

import json
import logging
import os
from typing import Any, Dict, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Assume insightsdb is an existing module with the InsightsDB class
# Replace the following import with the actual import statement as per your project structure
# For example:
# from insightsdb_module import insightsdb
# Here, we'll assume it's available in the global namespace
# If not, you need to adjust the import accordingly.
# For demonstration purposes, we'll define a mock InsightsDB class.
# Remove or replace this with the actual import in your environment.

class insightsdb:
    def __init__(self):
        # Initialize your actual database connection here
        import sqlite3
        self.connection = sqlite3.connect('data.db')  # Replace with actual DB
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()

    def _execute(self, query: str, params: tuple = ()) -> List[sqlite3.Row]:
        """
        Execute a SQL query with optional parameters.

        :param query: SQL query to execute.
        :param params: Tuple of parameters to pass with the query.
        :return: List of sqlite3.Row objects for SELECT queries, empty list otherwise.
        """
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
            if query.strip().upper().startswith("SELECT"):
                return self.cursor.fetchall()
            return []
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            return []

    def close(self):
        """Close the database connection."""
        self.connection.close()

class DataInserter:
    """
    Class responsible for inserting and updating data into the database using InsightsDB.
    """

    def __init__(self):
        """
        Initialize the DataInserter with an instance of InsightsDB.
        """
        # Initialize the database connection
        self.db = insightsdb()  # Replace with the actual initialization if different

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
        except KeyError as ke:
            logger.error(f"Missing unique key '{ke.args[0]}' for table '{table_name}'. Record: {record}. Skipping.")
            return

        # Construct WHERE clause for unique keys
        where_clause = " AND ".join([f"{key} = ?" for key in unique_keys])
        where_values = tuple(unique_values[key] for key in unique_keys)

        # Check if record exists
        select_query = f"SELECT * FROM {table_name} WHERE {where_clause} LIMIT 1;"
        try:
            result = self.db._execute(select_query, where_values)
        except Exception as e:
            logger.error(f"Error executing SELECT for table '{table_name}': {e}")
            return

        if result:
            # Assuming db._execute returns a list of sqlite3.Row objects
            existing_record = dict(result[0])  # Convert sqlite3.Row to dict
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
                        update_fields.append(f"{column} = ?")
                        update_values.append(serialized)
                    else:
                        update_fields.append(f"{column} = ?")
                        update_values.append(value)

                update_values.extend([unique_values[key] for key in unique_keys])

                update_query = f"UPDATE {table_name} SET {', '.join(update_fields)} WHERE {where_clause};"
                try:
                    self.db._execute(update_query, tuple(update_values))
                    logger.info(f"Updated record in '{table_name}': {unique_values}")
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
                insert_placeholders.append("?")
                if column in json_columns:
                    serialized = self.serialize_field(value)
                    insert_values.append(serialized)
                else:
                    insert_values.append(value)

            insert_query = f"INSERT INTO {table_name} ({', '.join(insert_fields)}) VALUES ({', '.join(insert_placeholders)});"
            try:
                self.db._execute(insert_query, tuple(insert_values))
                logger.info(f"Inserted new record into '{table_name}': {unique_values}")
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

            for record in records:
                if not isinstance(record, dict):
                    logger.warning(f"Record in table '{table_name}' is not a dictionary. Skipping.")
                    continue

                self.insert_or_update_record(table_name, record)

    def close(self):
        """Close the database connection."""
        self.db.close()
        logger.info("Database connection closed.")

def load_json_file(file_path: str) -> Dict[str, Any]:
    """
    Load JSON data from a file.

    :param file_path: Path to the JSON file.
    :return: Parsed JSON data as a dictionary.
    """
    if not os.path.isfile(file_path):
        logger.error(f"File '{file_path}' does not exist.")
        return {}

    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        logger.info(f"Successfully loaded data from '{file_path}'.")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from file '{file_path}': {e}")
    except Exception as e:
        logger.error(f"Unexpected error reading file '{file_path}': {e}")

    return {}

def main():
    # Define the fixed path to perf_data.json relative to the script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(script_dir, 'perf_data.json')

    # Load data from the specified JSON file
    data = load_json_file(json_file_path)
    if not data:
        logger.error("No data to process. Exiting.")
        return

    # Initialize the DataInserter
    inserter = DataInserter()

    # Insert data
    logger.info("Inserting data from perf_data.json:")
    inserter.insert_data(data)

    # Optionally, you can attempt to insert the same data again to test duplicate prevention and update logic
    # Uncomment the following lines if you want to perform this step
    """
    logger.info("\nInserting duplicate data from perf_data.json to test update logic:")
    inserter.insert_data(data)
    """

    # Insert additional data with modifications (if needed)
    # You can define another JSON file or modify the existing perf_data.json and rerun the script

    # Close the database connection
    inserter.close()

if __name__ == "__main__":
    main()
