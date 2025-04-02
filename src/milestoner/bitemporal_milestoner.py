import logging
from datetime import datetime
from typing import List, Dict, Any
import uuid

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Add formatter to console handler
console_handler.setFormatter(formatter)

# Add console handler to logger
logger.addHandler(console_handler)

STAGING_SCHEMA = 'STAGING'
CONFORMED_SCHEMA = 'CONFORMED'

# Global column names for temporal tracking
VALID_FROM_COL = 'VALID_FROM'
VALID_TO_COL = 'VALID_TO'
SYSTEM_FROM_COL = 'SYSTEM_FROM'
SYSTEM_TO_COL = 'SYSTEM_TO'
ROW_CHECKSUM_COL = 'ROW_CHECKSUM'
STAGING_GUID_COL = 'STAGING_GUID'
BATCH_ID_COL = 'BATCH_ID'
PROCESSED_DATETIME_COL = 'PROCESSED_DATETIME'
ROW_ADDED_DATETIME_COL = 'ROW_ADDED_DATETIME'
DATA_COL = 'DATA'
LOCKED_COL = 'LOCKED'
MILESTONING_FLAG_COL = 'MILESTONING_FLAG'

# Milestoning flag values
FLAG_DUPLICATE = 'DUPLICATE'
FLAG_INVALID_DATA = 'INVALID_DATA'
FLAG_MISSING_REQUIRED = 'MISSING_REQUIRED'
FLAG_PROCESSED = 'null'

class BitemporalMilestoner:
    """
    Handles the milestoning process for moving data from staging to conformed layer.
    
    The milestoner tracks both functional validity (when a record was true in the real world)
    and systematic validity (when we learned about each change) using bitemporal tracking.
    """
    
    def __init__(
        self,
        business_keys: List[str],
        temporal_column: str,
        data_columns: List[str]
    ):
        """
        Initialize the BitemporalMilestoner.
        
        Args:
            business_keys: List of columns that uniquely identify a record
            temporal_column: Name of the column containing the temporal value
            data_columns: List of columns that contain the data
        """
        self.business_keys = business_keys
        self.temporal_column = temporal_column
        self.data_columns = data_columns
        
        logger.info(f"Initialized BitemporalMilestoner with business keys: {business_keys}")
    
    def _get_lock_batch_query(
        self,
        staging_table: str,
        batch_id: str,
        batch_size: int
    ) -> str:
        """
        Generate SQL query to lock records for processing.
        
        Args:
            staging_table: Name of the staging table
            batch_id: ID of the current batch
            batch_size: Maximum number of records to process
            
        Returns:
            SQL query to lock records
        """
        return f"""
        UPDATE {STAGING_SCHEMA}.{staging_table}
        SET {LOCKED_COL} = '{batch_id}',
            {MILESTONING_FLAG_COL} = NULL
        WHERE {STAGING_GUID_COL} IN (
            SELECT {STAGING_GUID_COL}
            FROM {STAGING_SCHEMA}.{staging_table}
            WHERE {PROCESSED_DATETIME_COL} IS NULL
            AND {LOCKED_COL} IS NULL
            ORDER BY {ROW_ADDED_DATETIME_COL} ASC
            LIMIT {batch_size}
        )
        """
    
    def _get_duplicate_detection_query(
        self,
        staging_table: str,
        batch_id: str
    ) -> str:
        """
        Generate SQL query to identify duplicates within a batch.
        
        Args:
            staging_table: Name of the staging table
            batch_id: ID of the current batch
            
        Returns:
            SQL query to identify duplicates
        """
        return f"""
        UPDATE {STAGING_SCHEMA}.{staging_table} t
        SET {MILESTONING_FLAG_COL} = '{FLAG_DUPLICATE}'
        WHERE {STAGING_GUID_COL} IN (
            SELECT {STAGING_GUID_COL}
            FROM (
                SELECT {STAGING_GUID_COL},
                    ROW_NUMBER() OVER (
                        PARTITION BY {ROW_CHECKSUM_COL}
                        ORDER BY {ROW_ADDED_DATETIME_COL} ASC
                    ) as rn
                FROM {STAGING_SCHEMA}.{staging_table}
                WHERE {LOCKED_COL} = '{batch_id}'
            ) ranked_records
            WHERE rn > 1
        );
        """
    
    def _snake_to_camel(self, snake_str: str) -> str:
        """
        Convert snake_case string to camelCase.
        
        Args:
            snake_str: String in snake_case format
            
        Returns:
            String in camelCase format
        """
        components = snake_str.lower().split('_')
        return components[0] + ''.join(x.title() for x in components[1:])
    
    def _get_data_fields_select(self) -> str:
        """
        Generate the SELECT clause for extracting data fields from the variant column.
        
        Returns:
            String containing the SELECT clause for data fields
        """
        return ', '.join(
            f"DATA:{self._snake_to_camel(col)}::STRING as {col}"
            for col in self.data_columns
        )
    
    def _get_merge_query(
        self,
        staging_table: str,
        conformed_table: str,
        batch_id: str,
        current_time: datetime
    ) -> str:
        """
        Generate SQL query to merge records into conformed table.
        
        Args:
            staging_table: Name of the staging table
            conformed_table: Name of the conformed table
            batch_id: ID of the current batch
            current_time: Current timestamp for system time
            
        Returns:
            SQL query to merge records
        """
        # Extract data fields from variant column
        # Create unique staging records CTE
        unique_staging = f"""
        SELECT
            {self._get_data_fields_select()},
            {ROW_CHECKSUM_COL},
            {STAGING_GUID_COL}, 
            {ROW_ADDED_DATETIME_COL}
        FROM {STAGING_SCHEMA}.{staging_table}
        WHERE {LOCKED_COL} = '{batch_id}'
        AND {MILESTONING_FLAG_COL} IS NULL
        """
        
        # Use MERGE command for atomic updates
        return f"""
        BEGIN;
        
        -- Merge new/changed records
        MERGE INTO {conformed_table} t
        USING (
            {unique_staging}
        ) s
        ON {' AND '.join(f"t.{key} = s.{key}" for key in self.business_keys)}
        WHEN MATCHED AND t.{VALID_TO_COL} IS NULL AND t.{ROW_CHECKSUM_COL} != s.{ROW_CHECKSUM_COL} THEN
            UPDATE SET
                {VALID_TO_COL} = s.{self.temporal_column},
                {SYSTEM_TO_COL} = '{current_time}'
        WHEN NOT MATCHED THEN
            INSERT (
                {', '.join(self.data_columns + [
                    VALID_FROM_COL,
                    VALID_TO_COL,
                    SYSTEM_FROM_COL,
                    SYSTEM_TO_COL,
                    ROW_CHECKSUM_COL,
                    STAGING_GUID_COL,
                    BATCH_ID_COL
                ])}
            )
            VALUES (
                {', '.join(f"s.{col}" for col in self.data_columns)},
                s.{self.temporal_column},
                NULL,
                '{current_time}',
                NULL,
                s.{ROW_CHECKSUM_COL},
                s.{STAGING_GUID_COL},
                '{batch_id}'
            );
        
        -- Mark processed records
        UPDATE {STAGING_SCHEMA}.{staging_table}
        SET {PROCESSED_DATETIME_COL} = '{current_time}',
            {MILESTONING_FLAG_COL} = {FLAG_PROCESSED},
            {LOCKED_COL} = NULL,
            {BATCH_ID_COL} = '{batch_id}'
        WHERE {LOCKED_COL} = '{batch_id}';  
        
        COMMIT;
        """
    
    def process_batch(
        self,
        staging_table: str,
        conformed_table: str,
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Process a batch of staging records.
        
        Args:
            staging_table: Name of the staging table
            conformed_table: Name of the conformed table
            batch_size: Maximum number of records to process
            
        Returns:
            Dictionary containing:
                - batch_id: ID of the processed batch
                - records_processed: Number of records processed
                - duplicates_found: Number of duplicates found
                - records_inserted: Number of records inserted
                - records_closed: Number of records closed
        """
        current_time = datetime.now()
        batch_id = str(uuid.uuid4())
        
        logger.info(f"Starting batch {batch_id} with size {batch_size}")
        
        # Step 1: Get unprocessed records and lock them
        lock_query = self._get_lock_batch_query(
            staging_table,
            batch_id,
            batch_size
        )
        logger.info(f"Lock query: {lock_query}")
        
        # Step 2: Identify duplicates
        duplicate_query = self._get_duplicate_detection_query(
            staging_table,
            batch_id
        )
        logger.info(f"Duplicate detection query: {duplicate_query}")
        
        # Step 3: Merge records
        merge_query = self._get_merge_query(
            staging_table,
            conformed_table,
            batch_id,
            current_time
        )
        logger.info(f"Merge query: {merge_query}")
        
        # TODO: Execute queries in Snowflake and get results
        
        return {
            'batch_id': batch_id,
            'queries': {
                'lock': lock_query,
                'duplicates': duplicate_query,
                'merge': merge_query
            }
        }