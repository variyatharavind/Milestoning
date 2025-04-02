import pytest
import snowflake.connector
from datetime import datetime
import uuid
from src.milestoner.bitemporal_milestoner import BitemporalMilestoner

# Test configuration
TEST_STAGING_SCHEMA = 'STAGING'
TEST_CONFORMED_SCHEMA = 'CONFORMED'
TEST_SUFFIX = uuid.uuid4().hex[:8]
TEST_STAGING_TABLE = f'TEST_STAGING_{TEST_SUFFIX}'
TEST_CONFORMED_TABLE = f'TEST_CONFORMED_{TEST_SUFFIX}'

@pytest.fixture(scope="session")
def snowflake_conn():
    """Create a real Snowflake connection for testing."""
    conn = snowflake.connector.connect(
        user='YOUR_USER',
        password='YOUR_PASSWORD',
        account='YOUR_ACCOUNT',
        warehouse='TEST_WH',
        database='TEST_DATABASE'
    )
    yield conn
    conn.close()

@pytest.fixture(scope="session")
def setup_test_tables(snowflake_conn):
    """Set up test tables in Snowflake."""
    cursor = snowflake_conn.cursor()
    
    # Create staging table
    cursor.execute(f"""
    CREATE OR REPLACE TABLE {TEST_STAGING_SCHEMA}.{TEST_STAGING_TABLE} (
        DATA VARIANT,
        ROW_CHECKSUM STRING,
        STAGING_GUID STRING,
        BATCH_ID STRING,
        PROCESSED_DATETIME TIMESTAMP,
        ROW_ADDED_DATETIME TIMESTAMP,
        LOCKED STRING,
        MILESTONING_FLAG STRING
    )
    """)
    
    # Create conformed table
    cursor.execute(f"""
    CREATE OR REPLACE TABLE {TEST_CONFORMED_SCHEMA}.{TEST_CONFORMED_TABLE} (
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
    cursor.execute(f"DROP TABLE IF EXISTS {TEST_STAGING_SCHEMA}.{TEST_STAGING_TABLE}")
    cursor.execute(f"DROP TABLE IF EXISTS {TEST_CONFORMED_SCHEMA}.{TEST_CONFORMED_TABLE}")
    cursor.close()

@pytest.fixture
def milestoner():
    """Create a BitemporalMilestoner instance for testing."""
    return BitemporalMilestoner(
        business_keys=['USER_ID', 'EMAIL'],
        temporal_column='EFFECTIVE_DATE',
        data_columns=['USER_ID', 'EMAIL', 'FIRST_NAME', 'LAST_NAME', 'EFFECTIVE_DATE']
    )

def test_snake_to_camel(milestoner):
    """Test conversion from snake_case to camelCase."""
    test_cases = [
        ('USER_ID', 'userId'),
        ('FIRST_NAME', 'firstName'),
        ('LAST_NAME', 'lastName'),
        ('EMAIL_ADDRESS', 'emailAddress'),
        ('SINGLE', 'single')
    ]
    
    for input_str, expected in test_cases:
        assert milestoner._snake_to_camel(input_str) == expected

def test_get_data_fields_select(milestoner):
    """Test generation of data field selection clause."""
    expected = (
        "DATA:userId::STRING as USER_ID, "
        "DATA:email::STRING as EMAIL, "
        "DATA:firstName::STRING as FIRST_NAME, "
        "DATA:lastName::STRING as LAST_NAME"
    )
    assert milestoner._get_data_fields_select() == expected

def test_scenario_new_records(milestoner, snowflake_conn, setup_test_tables):
    """Test scenario: Processing new records with real Snowflake."""
    cursor = snowflake_conn.cursor()
    
    # Insert test data into staging
    cursor.execute(f"""
    INSERT INTO {TEST_STAGING_TABLE} (
        DATA,
        ROW_CHECKSUM,
        STAGING_GUID,
        ROW_ADDED_DATETIME
    )
    SELECT
        PARSE_JSON('{{
            "userId": "1",
            "email": "john@example.com",
            "firstName": "John",
            "lastName": "Doe",
            "effectiveDate": "2024-01-01"
        }}'),
        'abc123',
        'guid1',
        '2024-02-01 10:00:00'::TIMESTAMP
    """)
    
    # Process the batch
    result = milestoner.process_batch(
        staging_table=TEST_STAGING_TABLE,
        conformed_table=TEST_CONFORMED_TABLE,
        batch_size=100
    )
    
    # Verify the results
    cursor.execute(f"""
    SELECT * FROM {TEST_CONFORMED_SCHEMA}.{TEST_CONFORMED_TABLE}
    WHERE VALID_TO IS NULL
    """)
    final_state = cursor.fetchall()
    
    assert len(final_state) == 1
    assert final_state[0][1] == 'john@example.com'  # email
    assert final_state[0][2] == 'John'  # first_name
    assert final_state[0][3] == 'Doe'   # last_name
    assert final_state[0][4] == '2024-01-01'  # effective_date
    assert final_state[0][5] == '2024-01-01'  # valid_from
    assert final_state[0][6] is None  # valid_to
    
    # Verify staging record was marked as processed
    cursor.execute(f"""
    SELECT MILESTONING_FLAG, PROCESSED_DATETIME
    FROM {TEST_STAGING_SCHEMA}.{TEST_STAGING_TABLE}
    WHERE STAGING_GUID = 'guid1'
    """)
    staging_state = cursor.fetchall()
    assert staging_state[0][0] == 'PROCESSED'
    assert staging_state[0][1] is not None
    
    cursor.close()

def test_scenario_update_records(milestoner, snowflake_conn, setup_test_tables):
    """Test scenario: Processing updates to existing records with real Snowflake."""
    cursor = snowflake_conn.cursor()
    
    # Insert initial record
    cursor.execute(f"""
    INSERT INTO {TEST_STAGING_TABLE} (
        DATA,
        ROW_CHECKSUM,
        STAGING_GUID,
        ROW_ADDED_DATETIME
    )
    SELECT
        PARSE_JSON('{{
            "userId": "1",
            "email": "john@example.com",
            "firstName": "John",
            "lastName": "Doe",
            "effectiveDate": "2024-01-01"
        }}'),
        'abc123',
        'guid1',
        '2024-02-01 10:00:00'::TIMESTAMP
    """)
    
    # Process first batch
    milestoner.process_batch(TEST_STAGING_TABLE, TEST_CONFORMED_TABLE)
    
    # Insert updated record
    cursor.execute(f"""
    INSERT INTO {TEST_STAGING_TABLE} (
        DATA,
        ROW_CHECKSUM,
        STAGING_GUID,
        ROW_ADDED_DATETIME
    )
    SELECT
        PARSE_JSON('{{
            "userId": "1",
            "email": "john@example.com",
            "firstName": "Johnny",
            "lastName": "Doe",
            "effectiveDate": "2024-02-01"
        }}'),
        'def456',
        'guid2',
        '2024-02-01 11:00:00'::TIMESTAMP
    """)
    
    # Process second batch
    milestoner.process_batch(TEST_STAGING_TABLE, TEST_CONFORMED_TABLE)
    
    # Verify final state
    cursor.execute(f"""
    SELECT * FROM {TEST_CONFORMED_SCHEMA}.{TEST_CONFORMED_TABLE}
    WHERE VALID_TO IS NULL
    """)
    final_state = cursor.fetchall()
    
    assert len(final_state) == 1
    assert final_state[0][1] == 'john@example.com'  # email (unchanged)
    assert final_state[0][2] == 'Johnny'  # first_name (updated)
    assert final_state[0][3] == 'Doe'     # last_name (unchanged)
    assert final_state[0][4] == '2024-02-01'  # effective_date (updated)
    assert final_state[0][5] == '2024-02-01'  # valid_from (new version)
    assert final_state[0][6] is None  # valid_to (still active)
    
    cursor.close()

def test_scenario_duplicate_records(milestoner, snowflake_conn, setup_test_tables):
    """Test scenario: Handling duplicate records with real Snowflake."""
    cursor = snowflake_conn.cursor()
    
    # Insert duplicate records
    cursor.execute(f"""
    INSERT INTO {TEST_STAGING_TABLE} (
        DATA,
        ROW_CHECKSUM,
        STAGING_GUID,
        ROW_ADDED_DATETIME
    )
    SELECT
        PARSE_JSON('{{
            "userId": "1",
            "email": "john@example.com",
            "firstName": "John",
            "lastName": "Doe",
            "effectiveDate": "2024-01-01"
        }}'),
        'abc123',
        'guid1',
        '2024-02-01 10:00:00'::TIMESTAMP
    UNION ALL
    SELECT
        PARSE_JSON('{{
            "userId": "1",
            "email": "john@example.com",
            "firstName": "John",
            "lastName": "Doe",
            "effectiveDate": "2024-01-01"
        }}'),
        'abc123',
        'guid2',
        '2024-02-01 10:01:00'::TIMESTAMP
    """)
    
    # Process the batch
    milestoner.process_batch(TEST_STAGING_TABLE, TEST_CONFORMED_TABLE)
    
    # Verify only one record was processed
    cursor.execute(f"""
    SELECT COUNT(*) FROM {TEST_CONFORMED_SCHEMA}.{TEST_CONFORMED_TABLE}
    WHERE VALID_TO IS NULL
    """)
    conformed_count = cursor.fetchone()[0]
    assert conformed_count == 1
    
    # Verify one record was marked as duplicate
    cursor.execute(f"""
    SELECT COUNT(*) FROM {TEST_STAGING_SCHEMA}.{TEST_STAGING_TABLE}
    WHERE MILESTONING_FLAG = 'DUPLICATE'
    """)
    duplicate_count = cursor.fetchone()[0]
    assert duplicate_count == 1
    
    cursor.close()
