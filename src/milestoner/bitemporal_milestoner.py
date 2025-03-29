"""
Snowflake Bitemporal Milestoner for tracking both valid time and system time.
"""
from datetime import datetime
from typing import List, Dict, Any, Optional

class BitemporalMilestoner:
    # Snowflake temporal column names
    VALID_FROM_COL = 'VALID_FROM'
    VALID_TO_COL = 'VALID_TO'
    SYSTEM_FROM_COL = 'SYSTEM_FROM'
    SYSTEM_TO_COL = 'SYSTEM_TO'
    
    def __init__(
        self,
        business_keys: List[str],
        data_columns: List[str],
        temporal_column: str
    ):
        """
        Initialize the Snowflake Bitemporal Milestoner.
        
        Args:
            business_keys: List of columns that uniquely identify a record
            data_columns: List of columns containing data values to track changes for
            temporal_column: Name of the column containing the temporal value (date/time)
        """
        self.business_keys = business_keys
        self.data_columns = data_columns
        self.temporal_column = temporal_column
        
    def _create_key(self, record: Dict[str, Any]) -> str:
        """Create a unique key from business keys."""
        return '|'.join(str(record[key]) for key in self.business_keys)
    
    def _create_lookup(self, records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Create a lookup dictionary of current records.
        
        Current records are identified by SYSTEM_TO being NULL, indicating they are
        our current understanding of the data.
        """
        return {
            self._create_key(record): record 
            for record in records 
            if record.get(self.SYSTEM_TO_COL) is None
        }
    
    def _has_changes(self, new_record: Dict[str, Any], current_record: Dict[str, Any]) -> bool:
        """Check if data columns have changes."""
        return any(
            new_record.get(col) != current_record.get(col)
            for col in self.data_columns
        )
    
    def _get_current_timestamp(self) -> datetime:
        """Get current timestamp for system time."""
        return datetime.now()
        
    def process_record(
        self,
        staging_record: Dict[str, Any],
        conformed_records: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Process a single record from staging to conformed layer.
        
        Args:
            staging_record: New record from staging
            conformed_records: Existing records in conformed layer
            
        Returns:
            Dictionary containing:
                - new_record: New record to insert (if any)
                - update_records: Records that need updating (if any)
                - close_records: Records that need to be closed (if any)
        """
        current_timestamp = self._get_current_timestamp()
        result = {
            'new_record': None,
            'update_records': [],
            'close_records': []
        }
        
        # Create lookup of current records
        current_records = self._create_lookup(conformed_records)
        key = self._create_key(staging_record)
        
        # Handle new record case
        if key not in current_records:
            new_record = staging_record.copy()
            new_record.update({
                self.VALID_FROM_COL: staging_record[self.temporal_column],
                self.VALID_TO_COL: None,  # Current record
                self.SYSTEM_FROM_COL: current_timestamp,
                self.SYSTEM_TO_COL: None  # Current record
            })
            result['new_record'] = new_record
            return result
            
        # We'll implement other cases (updates, closures) in subsequent steps
        return result

    def get_snowflake_merge_sql(self, table_name: str) -> str:
        """
        Generate Snowflake MERGE statement for temporal table.
        
        Args:
            table_name: Name of the target table
            
        Returns:
            SQL MERGE statement
        """
        # We'll implement this in the next step
        pass 