#!/usr/bin/env python3

import json
import logging
from typing import Any, Dict, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
            existing_record = dict(result[0])  # Convert sqlite3.Row to dict if necessary
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

def main():
    # Initialize the DataInserter
    inserter = DataInserter()

    # Insert sample data
    logger.info("Inserting sample data:")
    inserter.insert_data(SAMPLE_DATA)

    # Attempt to insert the same data again to test duplicate prevention and update logic
    logger.info("\nInserting duplicate sample data to test update logic:")
    inserter.insert_data(SAMPLE_DATA)

    # Define additional data with modifications to test update functionality
    ADDITIONAL_DATA = {
        "unit_test_records": [
            {
                "repo_url": "https://github.cloud.test.com/perf",
                "github_org": "perf-test-v2",
                "business_application_id": "CI-perf-ba-id-01",
                "component_id": "CI-perf-com-id-01",
                "github_repo": "level",
                "github_branch": "main",
                "coverage": "0.8",  # Updated coverage
                "pipeline_id": "PERF01-test-pipe-line-id3127244037",
                "artifact_info": {"info": "updated"},
                "message_blob": {},
                "policy_result": "pass",  # Updated result
                "source": "Sonar",
                "artifact_url": "https://artifactory.cloud.test.com",
                "artifact_name": "perf_test_artifact",
                "artifact_version": "level",
                "policy_reasons": [{"errorCode": "TMM100", "errorMessage": "Sample error message updated"}]
            },
            {
                "repo_url": "https://github.cloud.test.com/perf",
                "github_org": "perf-test-v2",
                "business_application_id": "CI-perf-ba-id-02",
                "component_id": "CI-perf-com-id-02",
                "github_repo": "level2",
                "github_branch": "develop",
                "coverage": "0.75",
                "pipeline_id": "PERF02-test-pipe-line-id3127244038",
                "artifact_info": {},
                "message_blob": {},
                "policy_result": "fail",
                "source": "Sonar",
                "artifact_url": "https://artifactory.cloud.test.com",
                "artifact_name": "perf_test_artifact2",
                "artifact_version": "level2",
                "policy_reasons": [{"errorCode": "TMM101", "errorMessage": "Another error message"}]
            }
        ],
        "sevenps_result_set": [
            {
                "repo_url": "https://github.cloud.test.com/perf-test-",
                "artifact_url": "https://artifactory.cloud.test.com/b",
                "artifact_name": "perf_test_artifact",
                "artifact_version": "level4",
                "test_type": "component_tests",
                "test_request_id": "0000-1111-2222",  # Existing record with updates
                "asv": "ASVTEST_UPDATED",
                "bap": "BAPTEST",
                "report_doc": {"report": "updated"},
                "traceability_doc": {},
                "source": "enterprise_test_report_aggregates",
                "github_org": "perf-test",
                "github_repo": "level4",
                "github_branch": "feature"
            }
        ],
        "jira_issues": [
            {
                "jira_key": "TEST-4-cr-comp-auto",
                "repo_url": "https://github.cloud.test.com/perf-test-v2/1",
                "jira_status": "In Progress",  # Updated status
                "issue_data": {"data": "updated"},
                "bapci": "BAPTESTCI",
                "status": "In Progress",
                "category": "Component",
                "priority": "High",  # Updated priority
                "test_type": "Automated",
                "issue_key": "TEST-4-cr-comp-auto",
                "issuetype": "Test",
                "gitrepourl": "https://github.cloud.test.com/perf-test-v",
                "softwareversion": "1.1",  # Updated version
                "businesscapability": "Updated Capability",
                "issue_changed_timestamp": "2024-11-22 15:30:50.000000",
                "marked_for_delete": False,
                "source": "jira_collector"
            }
        ],
        "manual_test_records": [
            {
                "jira_key": "TEST-4-cr-ld-man",
                "artifact_url": "https://artifactory.cloud.test.com",
                "execution_result": [{"result": "passed"}],  # Updated execution result
                "repo_url": "https://github.cloud.test.com/perf-test",
                "issue_status": "Approved"
            }
        ]
    }

    # Insert additional data with modifications
    logger.info("\nInserting additional data with modifications:")
    inserter.insert_data(ADDITIONAL_DATA)

    # Close the database connection
    inserter.close()

if __name__ == "__main__":
    main()
