# Milestoner

A Python package for handling bitemporal data tracking in Snowflake.

## Design Decisions

1. Temporal Columns:
   - VALID_FROM: When the record becomes valid in the real world
   - VALID_TO: When the record ceases to be valid in the real world (NULL for current)
   - SYSTEM_FROM: When we first recorded this fact
   - SYSTEM_TO: When this record was superseded (NULL for current)

2. Current Records:
   - Both VALID_TO and SYSTEM_TO are NULL for current records
   - This is the Snowflake standard and provides clear semantics
   - NULL is more space-efficient than infinite timestamps
   - Makes querying current records straightforward (WHERE SYSTEM_TO IS NULL)

3. Temporal Tracking:
   - Valid Time: Tracks when something was/is true in the real world
   - System Time: Tracks when we learned about each change
