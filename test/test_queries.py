import pytest

from src.queries import *


def test_query_generation():
    # Define the expected SQL query as a string
    expected_sql = (
        "SELECT DISTINCT your_table.column1, your_table.column2, "
        "CASE WHEN (your_table.column3 ~ '[0-9]') THEN your_table.column3 ELSE '' END AS column3, "
        "your_table.column4 \n"
        "FROM your_table \n"
        "WHERE your_table.column3 IS NOT NULL AND your_table.column3 != '' AND "
        "EXTRACT(hour FROM your_table.column4) NOT BETWEEN 1 AND 2"
    )

    # Call the function to generate the query
    table_name = 'your_table'
    query = create_selectable(table_name)

    # Compile the query to a SQL string
    compiled_query = str(query.compile(compile_kwargs={"literal_binds": True}))
    print(compiled_query)

    # Remove extra whitespace and newlines for comparison
    compiled_query = ' '.join(compiled_query.split())
    expected_sql = ' '.join(expected_sql.split())

    # Assert that the generated SQL matches the expected SQL
    assert compiled_query == expected_sql

def test_create_selectable2():
    table_name = "test_table"
    query = create_selectable2(table_name)

    print('')
    print(str(query))
    compiled_query = str(query.compile(compile_kwargs={"literal_binds": True}))
    print(compiled_query)

    # Remove extra whitespace and newlines for comparison
    expected_query = '''SELECT DISTINCT test_table.column1, test_table.column2, CASE WHEN (test_table.column3 ~ '[0-9]') THEN test_table.column3 ELSE '' END AS column3, test_table.column4 
FROM test_table 
WHERE test_table.column3 IS NOT NULL AND test_table.column3 != '' AND EXTRACT(hour FROM test_table.column4) NOT BETWEEN 1 AND 2'''

    # Assert that the generated SQL matches the expected SQL
    assert compiled_query == expected_query


# Run the tests
if __name__ == "__main__":
    pytest.main()