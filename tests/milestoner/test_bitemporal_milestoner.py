"""
Tests for the BitemporalMilestoner class.
"""
import pytest
from datetime import datetime
from src.milestoner.bitemporal_milestoner import BitemporalMilestoner

@pytest.fixture
def milestoner():
    """Create a BitemporalMilestoner instance."""
    return BitemporalMilestoner(
        business_keys=['customer_id'],
        temporal_column='effective_date',
        checksum_column='ROW_CHECKSUM',
        row_added_column='ROW_ADDED_DATETIME',
        processed_column='PROCESSED_DATETIME',
        guid_column='GUID'
    )

def test_empty_batch(milestoner):
    """Test processing an empty batch."""
    result = milestoner.process_batch([])
    assert result['records_to_insert'] == []
    assert result['records_to_update'] == []
    assert result['records_to_close'] == []
    assert result['duplicate_records'] == []

def test_new_records(milestoner):
    """Test processing new records."""
    staging_records = [
        {
            'customer_id': 1,
            'name': 'John Doe',
            'effective_date': datetime(2024, 1, 1),
            'ROW_ADDED_DATETIME': datetime(2024, 1, 1),
            'ROW_CHECKSUM': 'abc123'  # Pre-computed checksum
        },
        {
            'customer_id': 2,
            'name': 'Jane Smith',
            'effective_date': datetime(2024, 1, 1),
            'ROW_ADDED_DATETIME': datetime(2024, 1, 1),
            'ROW_CHECKSUM': 'def456'  # Pre-computed checksum
        }
    ]
    
    result = milestoner.process_batch(staging_records)
    
    # Verify records to insert
    assert len(result['records_to_insert']) == 2
    for record in result['records_to_insert']:
        assert record['VALID_FROM'] == datetime(2024, 1, 1)
        assert record['VALID_TO'] is None
        assert record['SYSTEM_FROM'] is not None
        assert record['SYSTEM_TO'] is None
        assert record['GUID'] is not None
        assert record['PROCESSED_DATETIME'] is not None
        # Verify original checksum is preserved
        assert record['ROW_CHECKSUM'] in ['abc123', 'def456']
    
    # Verify no updates or closures
    assert result['records_to_update'] == []
    assert result['records_to_close'] == []
    assert result['duplicate_records'] == []

def test_duplicate_records(milestoner):
    """Test handling of duplicate records."""
    staging_records = [
        {
            'customer_id': 1,
            'name': 'John Doe',
            'effective_date': datetime(2024, 1, 1),
            'ROW_ADDED_DATETIME': datetime(2024, 1, 1),
            'ROW_CHECKSUM': 'abc123'
        },
        {
            'customer_id': 1,
            'name': 'John Doe',
            'effective_date': datetime(2024, 1, 1),
            'ROW_ADDED_DATETIME': datetime(2024, 1, 1),
            'ROW_CHECKSUM': 'abc123'
        }
    ]
    
    result = milestoner.process_batch(staging_records)
    
    # Verify one record to insert (first occurrence)
    assert len(result['records_to_insert']) == 1
    assert result['records_to_insert'][0]['customer_id'] == 1
    assert result['records_to_insert'][0]['ROW_CHECKSUM'] == 'abc123'
    
    # Verify one duplicate record
    assert len(result['duplicate_records']) == 1
    assert result['duplicate_records'][0]['customer_id'] == 1
    assert result['duplicate_records'][0]['ROW_CHECKSUM'] == 'abc123'
    
    # Verify no updates or closures
    assert result['records_to_update'] == []
    assert result['records_to_close'] == [] 