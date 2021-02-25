# 0.1.0
Initial release

# 0.2.0

## New implementations

### Select clause

- Added `_score` column. Available on not aggregated query
- DISTINCT implementation
- Functions
  - TO_CHAR(dateColumn, mask_date)
  - LATITUDE(geoPointColumn)
  - LONGITUDE(geoPointColumn)
- Aggregating expressions
  - AVG(numberColumn)
  - MIN(numberColumn)
  - MAX(numberColumn)
  - SUM(numberColumn)
  - COUNT(*)
  - COUNT(column)
  - COUNT(DISTINCT column) 

### Where clause

- IN condition implemented
- _esqlj for advanced filtering capabilities

### Group by clause
- implemented GROUP BY
- implemented group ordering

### Having clause
- implemented HAVING




