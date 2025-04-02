import pytest
from .conftest import TEST_STAGING_TABLE, TEST_CONFORMED_TABLE

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

def test_scenario_update_records(milestoner, insert_staging_data, verify_conformed_data, test_data):
    """Test scenario: Processing updates to existing records."""
    # Insert initial record
    insert_staging_data([test_data['samson_base_record']])
    milestoner.process_batch(TEST_STAGING_TABLE, TEST_CONFORMED_TABLE)
    
    # Insert and process update
    insert_staging_data([test_data['samson_name_correction']])
    milestoner.process_batch(TEST_STAGING_TABLE, TEST_CONFORMED_TABLE)
    
    # Verify final state
    verify_conformed_data([{
        'email': 'samson@example.com',
        'first_name': 'Samson',
        'last_name': 'Khess',
        'effective_date': '2024-02-01',
        'valid_from': '2024-02-01'
    }])

def test_scenario_duplicate_records(milestoner, insert_staging_data, verify_conformed_data, test_data):
    """Test scenario: Handling duplicate records."""
    # Insert duplicate records
    insert_staging_data([
        test_data['aravind_base_record'],
        test_data['aravind_duplicate_entry']
    ])
    
    # Process the batch
    milestoner.process_batch(TEST_STAGING_TABLE, TEST_CONFORMED_TABLE)
    
    # Verify only one record was processed
    verify_conformed_data([{
        'email': 'aravind@example.com',
        'first_name': 'Aravind',
        'last_name': 'Variyath',
        'effective_date': '2024-01-01',
        'valid_from': '2024-01-01'
    }])

def test_scenario_multiple_updates(milestoner, insert_staging_data, verify_conformed_data, test_data):
    """Test scenario: Processing multiple updates to the same record."""
    # Insert initial record
    insert_staging_data([test_data['aravind_base_record']])
    milestoner.process_batch(TEST_STAGING_TABLE, TEST_CONFORMED_TABLE)
    
    # Insert name correction
    insert_staging_data([test_data['aravind_name_correction']])
    milestoner.process_batch(TEST_STAGING_TABLE, TEST_CONFORMED_TABLE)
    
    # Insert email update
    insert_staging_data([test_data['aravind_email_update']])
    milestoner.process_batch(TEST_STAGING_TABLE, TEST_CONFORMED_TABLE)
    
    # Verify final state
    verify_conformed_data([{
        'email': 'aravind.variyath@example.com',
        'first_name': 'Aravind',
        'last_name': 'Variyath',
        'effective_date': '2024-03-01',
        'valid_from': '2024-03-01'
    }])

def test_scenario_multiple_people(milestoner, insert_staging_data, verify_conformed_data, test_data):
    """Test scenario: Processing records for multiple people simultaneously."""
    # Insert initial records for both people
    insert_staging_data([
        test_data['aravind_base_record'],
        test_data['samson_base_record']
    ])
    milestoner.process_batch(TEST_STAGING_TABLE, TEST_CONFORMED_TABLE)
    
    # Insert updates for both people
    insert_staging_data([
        test_data['aravind_email_update'],
        test_data['samson_name_correction']
    ])
    milestoner.process_batch(TEST_STAGING_TABLE, TEST_CONFORMED_TABLE)
    
    # Verify final state for both records
    verify_conformed_data([
        {
            'email': 'aravind.variyath@example.com',
            'first_name': 'Aravind',
            'last_name': 'Variyath',
            'effective_date': '2024-03-01',
            'valid_from': '2024-03-01'
        },
        {
            'email': 'samson@example.com',
            'first_name': 'Samson',
            'last_name': 'Khess',
            'effective_date': '2024-02-01',
            'valid_from': '2024-02-01'
        }
    ]) 