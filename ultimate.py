#!/usr/bin/env python3

import sys
import os
import json
import logging
import re
from typing import Any, Dict, List

# Add project root to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
sys.path.insert(0, project_root)

# Now you can import from src
from src.database import insightsdb

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def normalize_json_value(value_str: str) -> Any:
    """
    Attempt to parse a JSON string into a Python object.
    If parsing fails, try replacing single quotes with double quotes and parse again.
    If it still fails, return the original string.
    """
    value_str = value_str.strip()
    if value_str == "":
        return None  # Empty value

    # First attempt
    try:
        return json.loads(value_str)
    except json.JSONDecodeError:
        # Attempt to fix quotes by replacing single quotes with double quotes
        fixed_str = value_str.replace("'", '"')
        try:
            return json.loads(fixed_str)
        except json.JSONDecodeError:
            # Could not parse as JSON
            logger.debug(f"Could not parse value as JSON: {value_str}")
            return value_str

def ensure_parsed_json(value: Any) -> Any:
    """
    Ensure the given value is parsed as JSON.
    If it's already a dict or list, return as is.
    If it's a string, try to parse it as JSON using normalize_json_value().
    Otherwise, return as is.
    """
    if isinstance(value, dict) or isinstance(value, list):
        # Already a dict or list
        return value
    elif isinstance(value, str):
        # Try to parse the string
        return normalize_json_value(value)
    else:
        # Some other type (int, None, etc.), just return as is
        return value

class DataInserter:
    """
    Class responsible for inserting or updating data into the database using InsightsDB.
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

        # Initialize overall counters
        self.total_records = 0
        self.total_insertions = 0
        self.total_updates = 0
        self.total_skips = 0  # Total number of records skipped
        self.total_failures = 0  # Total number of insert/update failures

        # Initialize table-specific statistics
        self.table_stats = {
            table: {"total": 0, "insertions": 0, "updates": 0, "skips": 0, "failures": 0}
            for table in self.table_configs.keys()
        }

    def serialize_field(self, value: Any) -> str:
        """
        Serialize a field to JSON if it's a dict or list, else return as string.
        """
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        elif value is None:
            return ""
        else:
            return str(value)

    def clean_record_keys(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove leading and trailing spaces from the keys of a record.
        """
        cleaned_record = {k.strip(): v for k, v in record.items()}
        return cleaned_record

    def validate_table_name(self, table_name: str) -> bool:
        """
        Validate that the table name matches the expected pattern.
        """
        pattern = r'^[A-Za-z0-9_]+$'
        if re.match(pattern, table_name):
            return True
        else:
            logger.error(
                f"Invalid table name '{table_name}'. Table names should contain only alphanumeric characters and underscores."
            )
            return False

    def records_differ(self, existing_record: Dict[str, Any], new_record: Dict[str, Any],
                       json_columns: List[str], excluded_columns: List[str]) -> bool:
        """
        Compare fields (excluding excluded_columns) between existing and new records to determine if they differ.
        
        For JSON columns, parse both existing and new values into Python objects and compare.
        """
        for column, new_value in new_record.items():
            if column in excluded_columns:
                continue
            serialized_new_value = self.serialize_field(new_value)
            existing_value = existing_record.get(column, "")

            if column in json_columns:
                # Normalize both sides to Python objects
                parsed_existing = ensure_parsed_json(existing_value)
                parsed_new = ensure_parsed_json(serialized_new_value)
                if parsed_existing != parsed_new:
                    logger.debug(
                        f"Difference found in JSON column '{column}': existing='{parsed_existing}' vs new='{parsed_new}'"
                    )
                    return True
            else:
                # Normal column comparison
                if existing_value != serialized_new_value:
                    logger.debug(
                        f"Difference found in column '{column}': existing='{existing_value}' vs new='{serialized_new_value}'"
                    )
                    return True

        return False

    def insert_or_update_record(self, table_name: str, record: Dict[str, Any]):
        """
        Insert a new record or update an existing record in the specified table.
        """
        # Validate table name
        if not self.validate_table_name(table_name):
            logger.error(f"Table name '{table_name}' is invalid. Skipping record.")
            return

        # Clean the record keys
        cleaned_record = self.clean_record_keys(record)
        logger.debug(f"Cleaned record keys for table '{table_name}': {list(cleaned_record.keys())}")

        # Increment counters
        self.total_records += 1
        self.table_stats[table_name]["total"] += 1

        config = self.table_configs.get(table_name)
        if not config:
            logger.warning(f"No configuration found for table '{table_name}'. Skipping record.")
            self.total_skips += 1
            self.table_stats[table_name]["skips"] += 1
            return

        unique_keys = config["unique_keys"]
        excluded_columns = config["excluded_columns"]
        json_columns = config.get("json_columns", [])

        # Extract unique key values
        try:
            unique_values = {key: cleaned_record[key] for key in unique_keys}
            logger.debug(f"Unique keys for table '{table_name}': {unique_values}")
        except KeyError as ke:
            logger.error(f"Missing unique key '{ke.args[0]}' for table '{table_name}'. Record: {record}. Counting as failure.")
            self.total_failures += 1
            self.table_stats[table_name]["failures"] += 1
            return

        # Construct WHERE clause for unique keys
        where_clause = " AND ".join([f"{key} = %s" for key in unique_keys])
        where_values = tuple(unique_values[key] for key in unique_keys)

        # Determine which columns to select explicitly
        all_columns = [col for col in cleaned_record.keys() if col not in excluded_columns]
        select_columns = list(set(all_columns + unique_keys))

        # Perform SELECT with explicit columns
        select_query = f"SELECT {', '.join(select_columns)} FROM {table_name} WHERE {where_clause} LIMIT 1;"
        try:
            logger.debug(f"Executing SELECT query: {select_query} with values {where_values}")
            result = self.db._fetch_all(select_query, where_values)
            logger.info(f"SELECT query executed successfully. Result: {result}")
        except Exception as e:
            logger.error(f"Error executing SELECT for table '{table_name}': {e}")
            self.total_failures += 1
            self.table_stats[table_name]["failures"] += 1
            return

        if result:
            # Existing record found
            existing_record = dict(zip(select_columns, result[0]))
            logger.debug(f"Existing record found in '{table_name}': {existing_record}")

            # Check if fields differ
            if self.records_differ(existing_record, cleaned_record, json_columns, excluded_columns):
                # Prepare fields for update
                update_fields = []
                update_values = []
                for column, value in cleaned_record.items():
                    if column in excluded_columns:
                        continue
                    serialized_value = self.serialize_field(value)
                    existing_serialized = existing_record.get(column, "")

                    if column in json_columns:
                        parsed_existing = ensure_parsed_json(existing_serialized)
                        parsed_new = ensure_parsed_json(serialized_value)
                        if parsed_existing != parsed_new:
                            logger.info(
                                f"Updating JSON column '{column}' in '{table_name}': from '{parsed_existing}' to '{parsed_new}'"
                            )
                            update_fields.append(f"{column} = %s")
                            update_values.append(serialized_value)
                    else:
                        if existing_serialized != serialized_value:
                            logger.info(
                                f"Updating column '{column}' in '{table_name}': from '{existing_serialized}' to '{serialized_value}'"
                            )
                            update_fields.append(f"{column} = %s")
                            update_values.append(serialized_value)

                if update_fields:
                    update_values.extend([unique_values[key] for key in unique_keys])
                    update_query = f"UPDATE {table_name} SET {', '.join(update_fields)} WHERE {where_clause};"
                    try:
                        logger.debug(f"Executing UPDATE query: {update_query} with values {update_values}")
                        self.db._execute(update_query, tuple(update_values))
                        logger.info(f"Updated record in '{table_name}': {unique_values}")
                        self.total_updates += 1
                        self.table_stats[table_name]["updates"] += 1
                    except Exception as e:
                        logger.error(f"Error updating record in '{table_name}': {e}")
                        self.total_failures += 1
                        self.table_stats[table_name]["failures"] += 1
                else:
                    logger.info(f"No changes detected for record in '{table_name}': {unique_values}. Skipping update.")
                    self.total_skips += 1
                    self.table_stats[table_name]["skips"] += 1
            else:
                # No differences
                logger.info(f"No changes detected for record in '{table_name}': {unique_values}. Skipping update.")
                self.total_skips += 1
                self.table_stats[table_name]["skips"] += 1
        else:
            # No existing record, perform insertion
            insert_fields = []
            insert_placeholders = []
            insert_values = []
            for column, value in cleaned_record.items():
                if column in excluded_columns:
                    continue
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
                self.total_insertions += 1
                self.table_stats[table_name]["insertions"] += 1
            except Exception as e:
                logger.error(f"Error inserting record into '{table_name}': {e}")
                self.total_failures += 1
                self.table_stats[table_name]["failures"] += 1

    def insert_data(self, data: Dict[str, List[Dict[str, Any]]]):
        """
        Insert or update records into their respective tables.
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

        # After processing all records, log the overall and table-specific statistics
        logger.info("----- Data Insertion Statistics -----")
        logger.info(f"Total Records Processed: {self.total_records}")
        logger.info(f"Total Insertions: {self.total_insertions}")
        logger.info(f"Total Updates: {self.total_updates}")
        logger.info(f"Total Skips: {self.total_skips}")
        logger.info(f"Total Failures: {self.total_failures}")
        logger.info("-------------------------------------")

        # Log table-specific statistics
        logger.info("----- Table-Specific Statistics -----")
        for table, stats in self.table_stats.items():
            logger.info(f"Table: {table}")
            logger.info(f"  Total Records Processed: {stats['total']}")
            logger.info(f"  Insertions: {stats['insertions']}")
            logger.info(f"  Updates: {stats['updates']}")
            logger.info(f"  Skips: {stats['skips']}")
            logger.info(f"  Failures: {stats['failures']}")
        logger.info("-------------------------------------")

    def close(self):
        """Close the database connection."""
        self.db.close()
        logger.info("Database connection closed.")


def load_json_file(file_path: str) -> Dict[str, Any]:
    """
    Load JSON data from a file.
    """
    if not os.path.isfile(file_path):
        logger.error(f"File '{file_path}' does not exist.")
        return {}

    try:
        with open(file_path, "r") as f:
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
    json_file_path = os.path.join(script_dir, "perf_data.json")

    # Load data from the specified JSON file
    data = load_json_file(json_file_path)
    if not data:
        logger.error("No data to process. Exiting.")
        return

    # Initialize the DataInserter
    inserter = DataInserter()

    # Insert or update data
    logger.info("Inserting data from perf_data.json:")
    inserter.insert_data(data)

    # Close the database connection
    inserter.close()


if __name__ == "__main__":
    main()
