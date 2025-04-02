import pytest
from src.milestoner.bitemporal_milestoner import BitemporalMilestoner

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
        "DATA:lastName::STRING as LAST_NAME, "
        "DATA:effectiveDate::STRING as EFFECTIVE_DATE"
    )
    assert milestoner._get_data_fields_select() == expected 