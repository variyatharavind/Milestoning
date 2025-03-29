"""
Tests for the BitemporalMilestoner class.
"""
import pytest
from datetime import datetime
from src.milestoner import BitemporalMilestoner

def test_new_record():
    """Test handling of a new record."""
    # Setup
    business_keys = ['id']
    data_columns = ['value']
    temporal_column = 'effective_date'
    milestoner = BitemporalMilestoner(business_keys, data_columns, temporal_column)
    
    # Test data
    staging_record = {
        'id': 1,
        'value': 'test',
        'effective_date': datetime(2024, 1, 1)
    }
    conformed_records = []
    
    # Execute
    result = milestoner.process_record(staging_record, conformed_records)
    
    # Verify
    assert result['new_record'] is not None
    assert result['update_records'] == []
    assert result['close_records'] == []
    
    new_record = result['new_record']
    assert new_record['id'] == 1
    assert new_record['value'] == 'test'
    assert new_record['VALID_FROM'] == datetime(2024, 1, 1)
    assert new_record['VALID_TO'] is None
    assert new_record['SYSTEM_FROM'] is not None
    assert new_record['SYSTEM_TO'] is None

def test_no_changes():
    """Test handling of a record with no changes."""
    # Setup
    business_keys = ['id']
    data_columns = ['value']
    temporal_column = 'effective_date'
    milestoner = BitemporalMilestoner(business_keys, data_columns, temporal_column)
    
    # Test data
    staging_record = {
        'id': 1,
        'value': 'test',
        'effective_date': datetime(2024, 1, 1)
    }
    conformed_records = [{
        'id': 1,
        'value': 'test',
        'effective_date': datetime(2024, 1, 1),
        'VALID_FROM': datetime(2024, 1, 1),
        'VALID_TO': None,
        'SYSTEM_FROM': datetime(2024, 1, 1),
        'SYSTEM_TO': None
    }]
    
    # Execute
    result = milestoner.process_record(staging_record, conformed_records)
    
    # Verify
    assert result['new_record'] is None
    assert result['update_records'] == []
    assert result['close_records'] == [] 