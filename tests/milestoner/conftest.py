import pytest
import snowflake.connector
import uuid
import json
import os
from datetime import datetime

# Test configuration
TEST_STAGING_SCHEMA = 'STAGING'
TEST_CONFORMED_SCHEMA = 'CONFORMED'
TEST_SUFFIX = '' # uuid.uuid4().hex[:8]
TEST_STAGING_TABLE = f'TEST_STAGING_{TEST_SUFFIX}'
TEST_CONFORMED_TABLE = f'TEST_CONFORMED_{TEST_SUFFIX}'

def load_credentials():
    """
    Load credentials from the credentials.json file for local development testing.
    
    Returns:
        Dictionary containing the credentials
    """
    try:
        # Get the directory of the current file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up to the project root
        project_root = os.path.dirname(os.path.dirname(current_dir))
        # Path to the credentials file
        credentials_path = os.path.join(project_root, 'credentials.json')
        
        if not os.path.exists(credentials_path):
            print(f"Credentials file not found at {credentials_path}")
            return None
        
        with open(credentials_path, 'r') as f:
            credentials = json.load(f)
        
        print("Local development credentials loaded successfully")
        return credentials.get('snowflake')
    except Exception as e:
        print(f"Error loading credentials: {str(e)}")
        return None

@pytest.fixture(scope="session")
def snowflake_conn():
    """Create a real Snowflake connection for testing."""

    credentials = load_credentials()
    
    conn = snowflake.connector.connect(
        user=credentials.get('user'),
        password=credentials.get('password'),
        account=credentials.get('account'),
        warehouse=credentials.get('warehouse'),
        database=credentials.get('database'),
        role=credentials.get('role')
    )

    yield conn

    conn.close()

@pytest.fixture(scope="session")
def setup_test_tables(snowflake_conn):
    """Set up test tables in Snowflake."""
    cursor = snowflake_conn.cursor()
    
    # Create staging table
    # CREATE OR REPLACE TABLE {TEST_STAGING_SCHEMA}.{TEST_STAGING_TABLE} (
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {TEST_STAGING_SCHEMA}.{TEST_STAGING_TABLE} (
        DATA VARIANT,
        ROW_CHECKSUM STRING,
        STAGING_GUID STRING DEFAULT UUID_STRING(),
        BATCH_ID STRING,
        PROCESSED_DATETIME TIMESTAMP,
        ROW_ADDED_DATETIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        LOCKED STRING,
        MILESTONING_FLAG STRING
    )
    """)
    
    # Create conformed table
    # CREATE OR REPLACE TABLE {TEST_CONFORMED_SCHEMA}.{TEST_CONFORMED_TABLE} (
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {TEST_CONFORMED_SCHEMA}.{TEST_CONFORMED_TABLE} (
        USER_ID STRING,
        EMAIL STRING,
        FIRST_NAME STRING,
        LAST_NAME STRING,
        EFFECTIVE_DATE DATE,
        VALID_FROM DATE,
        VALID_TO DATE,
        SYSTEM_FROM TIMESTAMP,
        SYSTEM_TO TIMESTAMP,
        ROW_CHECKSUM STRING,
        STAGING_GUID STRING,
        BATCH_ID STRING
    )
    """)
    
    yield
    
    # Cleanup
    # cursor.execute(f"DROP TABLE IF EXISTS {TEST_STAGING_SCHEMA}.{TEST_STAGING_TABLE}")
    # cursor.execute(f"DROP TABLE IF EXISTS {TEST_CONFORMED_SCHEMA}.{TEST_CONFORMED_TABLE}")
    cursor.close()

@pytest.fixture
def milestoner():
    """Create a BitemporalMilestoner instance for testing."""
    from src.milestoner.bitemporal_milestoner import BitemporalMilestoner
    return BitemporalMilestoner(
        business_keys=['USER_ID', 'EMAIL'],
        temporal_column='EFFECTIVE_DATE',
        data_columns=['USER_ID', 'EMAIL', 'FIRST_NAME', 'LAST_NAME', 'EFFECTIVE_DATE']
    )

@pytest.fixture
def test_data():
    """Common test data for scenarios."""
    return {
        'aravind_base_record': {
            'data': {
                'userId': '1',
                'email': 'aravind@example.com',
                'firstName': 'Aravind',
                'lastName': 'Variyath',
                'effectiveDate': '2024-01-01'
            },
            'row_checksum': 'abc123',
            'staging_guid': 'guid1',
            'row_added_datetime': '2024-02-01 10:00:00'
        },
        'aravind_name_correction': {
            'data': {
                'userId': '1',
                'email': 'aravind@example.com',
                'firstName': 'Aravind',
                'lastName': 'Variyath',
                'effectiveDate': '2024-02-01'
            },
            'row_checksum': 'def456',
            'staging_guid': 'guid2',
            'row_added_datetime': '2024-02-01 11:00:00'
        },
        'aravind_email_update': {
            'data': {
                'userId': '1',
                'email': 'aravind.variyath@example.com',
                'firstName': 'Aravind',
                'lastName': 'Variyath',
                'effectiveDate': '2024-03-01'
            },
            'row_checksum': 'ghi789',
            'staging_guid': 'guid3',
            'row_added_datetime': '2024-02-01 12:00:00'
        },
        'aravind_duplicate_entry': {
            'data': {
                'userId': '1',
                'email': 'aravind@example.com',
                'firstName': 'Aravind',
                'lastName': 'Variyath',
                'effectiveDate': '2024-01-01'
            },
            'row_checksum': 'abc123',  # Same checksum as base record
            'staging_guid': 'guid4',
            'row_added_datetime': '2024-02-01 13:00:00'
        },
        'samson_base_record': {
            'data': {
                'userId': '2',
                'email': 'samson@example.com',
                'firstName': 'Samson',
                'lastName': 'Khess',
                'effectiveDate': '2024-01-01'
            },
            'row_checksum': 'jkl012',
            'staging_guid': 'guid5',
            'row_added_datetime': '2024-02-01 14:00:00'
        },
        'samson_name_correction': {
            'data': {
                'userId': '2',
                'email': 'samson@example.com',
                'firstName': 'Samson',
                'lastName': 'Smyth',
                'effectiveDate': '2024-02-01'
            },
            'row_checksum': 'mno345',
            'staging_guid': 'guid6',
            'row_added_datetime': '2024-02-01 15:00:00'
        },
        'samson_department_update': {
            'data': {
                'userId': '2',
                'email': 'samson@example.com',
                'firstName': 'Samson',
                'lastName': 'Smyth',
                'department': 'Engineering',  # New field
                'effectiveDate': '2024-03-01'
            },
            'row_checksum': 'pqr678',
            'staging_guid': 'guid7',
            'row_added_datetime': '2024-02-01 16:00:00'
        },
        'samson_duplicate_entry': {
            'data': {
                'userId': '2',
                'email': 'samson@example.com',
                'firstName': 'Samson',
                'lastName': 'Khess',
                'effectiveDate': '2024-01-01'
            },
            'row_checksum': 'jkl012',  # Same checksum as base record
            'staging_guid': 'guid8',
            'row_added_datetime': '2024-02-01 17:00:00'
        }
    }

@pytest.fixture
def insert_staging_data(snowflake_conn, setup_test_tables):
    """Helper fixture to insert data into staging table."""
    def _insert(data_list):
        cursor = snowflake_conn.cursor()
        for data in data_list:
            cursor.execute(f"""
            INSERT INTO {TEST_STAGING_SCHEMA}.{TEST_STAGING_TABLE} (
                DATA,
                ROW_CHECKSUM,
                STAGING_GUID,
                ROW_ADDED_DATETIME
            )
            SELECT
                PARSE_JSON('{json.dumps(data["data"])}'),
                '{data["row_checksum"]}',
                '{data["staging_guid"]}',
                '{data["row_added_datetime"]}'::TIMESTAMP
            """)
        cursor.close()
    return _insert

@pytest.fixture
def verify_conformed_data(snowflake_conn, setup_test_tables):
    """Helper fixture to verify data in conformed table."""
    def _verify(expected_records):
        cursor = snowflake_conn.cursor()
        cursor.execute(f"""
        SELECT * FROM {TEST_CONFORMED_SCHEMA}.{TEST_CONFORMED_TABLE}
        WHERE VALID_TO IS NULL
        """)
        actual_records = cursor.fetchall()
        print(actual_records)
        cursor.close()
        
        assert len(actual_records) == len(expected_records)
        for actual, expected in zip(actual_records, expected_records):
            assert actual[1] == expected['email']
            assert actual[2] == expected['first_name']
            assert actual[3] == expected['last_name']
            assert actual[4] == expected['effective_date']
            assert actual[5] == expected['valid_from']
            assert actual[6] is None  # valid_to
    return _verify 