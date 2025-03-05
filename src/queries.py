
from sqlalchemy import *
from sqlalchemy.sql import table, column

QUERY_TABLE = 'SELECT * FROM test_table'

QUERY_TRANSFORM_1 = """SELECT DISTINCT
    column1,
    column2,
    CASE 
        WHEN column3 ~ '[0-9]' THEN column3 
        ELSE '' 
    END AS column3,
    column4
FROM test_table
WHERE 
    column3 IS NOT NULL AND column3 <> '' 
    AND (EXTRACT(HOUR FROM column4) NOT BETWEEN 1 AND 2"""

QUERY_TRANSFORM_2 = """WITH Deduplicated AS (
    SELECT DISTINCT ON (column1, column2, column3, column4) *
    FROM test_table
    ORDER BY column1, column2, column3, column4
)
SELECT 
    column1, 
    column2, 
    CASE 
        WHEN column3 ~ '[0-9]' THEN column3 
        ELSE '' 
    END AS column3, 
    column4
FROM Deduplicated
WHERE column3 IS NOT NULL AND column3 <> '' 
AND EXTRACT(HOUR FROM column4) NOT BETWEEN 1 AND 2"""

QUERY_AGGREGATE="""SELECT
    DATE_TRUNC('hour', column4) AS hour_column4, 
    COUNT(DISTINCT column3) AS unique_column3_count, 
    AVG(column1) AS mean_column1, 
    AVG(column2) AS mean_column2, 
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY column1) AS median_column1, 
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY column2) AS median_column2 
FROM
    test_table
GROUP BY
    1
ORDER BY
    1"""

QUERY_TRANSFORM_AND_AGGREGATE='''WITH Deduplicated AS (
    SELECT DISTINCT ON (column1, column2, column3, column4) *
    FROM test_table
    ORDER BY column1, column2, column3, column4
), Transformed AS (
  SELECT 
      column1, 
      column2, 
      CASE 
          WHEN column3 ~ '[0-9]' THEN column3 
          ELSE '' 
      END AS column3, 
      column4
  FROM Deduplicated
  WHERE column3 IS NOT NULL AND column3 <> '' 
  AND EXTRACT(HOUR FROM column4) NOT BETWEEN 1 AND 2
), Aggregated AS (
  SELECT
    DATE_TRUNC('hour', column4) AS hour_column4, 
    COUNT(DISTINCT column3) AS unique_column3_count, 
    AVG(column1) AS mean_column1, 
    AVG(column2) AS mean_column2, 
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY column1) AS median_column1, 
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY column2) AS median_column2 
  FROM Transformed
  GROUP BY 1
  ORDER BY 1
 )
 SELECT *
 FROM Transformed t
 	LEFT OUTER JOIN Aggregated a on DATE_TRUNC('hour', t.column4) = a.hour_column4 '''

def create_transformation_selectable(tableName):
    # Define metadata and table dynamically
    metadata = MetaData()
    test_table = Table(
        tableName, metadata,
        Column('column1', String),
        Column('column2', String),
        Column('column3', String),
        Column('column4', DateTime)
    )

    # Define the CASE statement
    case_statement = case(
        (test_table.c.column3.op('~')('[0-9]'), test_table.c.column3),
        else_=''
    ).label('column3')

    # Define the WHERE clause
    where_clause = and_(
        test_table.c.column3.isnot(None),
        test_table.c.column3 != '',
        not_(extract('hour', test_table.c.column4).between(1, 2))
    )

    # Create the selectable
    query = select(
        test_table.c.column1,
        test_table.c.column2,
        case_statement,
        test_table.c.column4
    ).where(where_clause).distinct()

    return query


def create_transformation_selectable2(table_name):
    metadata = MetaData()
    test_table = Table(
        table_name, metadata,
        Column('column1', String),
        Column('column2', String),
        Column('column3', String),
        Column('column4', DateTime)
    )

    stmt = (
        select(
            test_table.c.column1,
            test_table.c.column2,
            case(
                (test_table.c.column3.op('~')('[0-9]'), test_table.c.column3),
                else_=''
            ).label("column3"),
            test_table.c.column4
        )
        .where(
            and_(
                test_table.c.column3.isnot(None),
                test_table.c.column3 != '',
                not_(extract('hour', test_table.c.column4).between(1, 2))
            )
        ).distinct()
    )

    return stmt


def create_selectable_for_column(column, table_name):
    metadata = MetaData()
    test_table = Table(
        table_name, metadata,
        Column(column, Float)
    )

    stmt = (
        select(
            test_table.c.column
        )
    )

    return stmt
