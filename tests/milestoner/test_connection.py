import pytest
from .conftest import TEST_STAGING_TABLE, TEST_CONFORMED_TABLE

def test_snowflake_connection(snowflake_conn):
    """Test that we can connect to Snowflake and run a simple query."""
    cursor = snowflake_conn.cursor()
    try:
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1, "Expected SELECT 1 to return 1"
    finally:
        cursor.close()

def test_scenario_new_records(milestoner, insert_staging_data, verify_conformed_data, test_data):
    """Test scenario: Processing new records."""
    # Insert test data
    insert_staging_data([test_data['aravind_base_record']])
    
    # Process the batch
    milestoner.process_batch(
        staging_table=TEST_STAGING_TABLE,
        conformed_table=TEST_CONFORMED_TABLE
    )
    
    # Verify results
    verify_conformed_data([{
        'email': 'aravind@example.com',
        'first_name': 'Aravind',
        'last_name': 'Variyath',
        'effective_date': '2024-01-01',
        'valid_from': '2024-01-01'
    }])

def test_insert_duplicate_records(insert_staging_data, test_data):
    """Test scenario: Inserting duplicate records."""
    # Insert test data
    insert_staging_data([test_data['aravind_base_record']])
    insert_staging_data([test_data['aravind_duplicate_entry']])
    
# def test_clear_tables(snowflake_conn):
#     """Clear all tables in the test database."""
#     cursor = snowflake_conn.cursor()
#     cursor.execute(f"DROP TABLE IF EXISTS STAGING.{TEST_STAGING_TABLE}")
#     cursor.execute(f"DROP TABLE IF EXISTS CONFORMED.{TEST_CONFORMED_TABLE}")
#     cursor.close()

