import sqlite3
import pandas as pd 

def create_database():
    """Create a SQLite database and tables."""
    conn = sqlite3.connect('STAFF.db')
    cursor = conn.cursor()

    # Create the INSTRUCTOR table
    cursor.execute('''CREATE TABLE IF NOT EXISTS INSTRUCTOR
                    (ID INT PRIMARY KEY NOT NULL,
                    FNAME TEXT NOT NULL,
                    LNAME TEXT NOT NULL,
                    CITY TEXT,
                    CCODE TEXT)''')

    # Commit changes and close connection
    conn.commit()
    conn.close()

def load_data_from_csv(file_path, table_name, attribute_list):
    """Load data from a CSV file into a SQLite table."""
    conn = sqlite3.connect('STAFF.db')

    # Read CSV into DataFrame
    df = pd.read_csv(file_path, names=attribute_list)

    # Save DataFrame to SQLite database
    df.to_sql(table_name, conn, if_exists='replace', index=False)

    # Close connection
    conn.close()

def append_data_to_table(data_dict, table_name):
    """Append data to an existing SQLite table."""
    conn = sqlite3.connect('STAFF.db')

    # Create DataFrame from data_dict
    data_append = pd.DataFrame(data_dict)

    # Append DataFrame to SQLite table
    data_append.to_sql(table_name, conn, if_exists='append', index=False)

    # Close connection
    conn.close()

def execute_query(query_statement):
    """Execute a SQL query and return the result."""
    conn = sqlite3.connect('STAFF.db')

    # Execute query
    query_output = pd.read_sql(query_statement, conn)

    # Close connection
    conn.close()

    return query_output

if __name__ == "__main__":
    # Define file path, table name, and attribute list
    file_path = '/home/project/INSTRUCTOR.csv'
    table_name = 'INSTRUCTOR'
    attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

    # Create the database and table if they don't exist
    create_database()

    # Load data from CSV into the database
    load_data_from_csv(file_path, table_name, attribute_list)

    # Append additional data to the table
    data_dict = {'ID': [100],
                'FNAME': ['John'],
                'LNAME': ['Doe'],
                'CITY': ['Paris'],
                'CCODE': ['FR']}
    append_data_to_table(data_dict, table_name)

    # Execute a query to count the number of rows in the table
    query_statement = f"SELECT COUNT(*) FROM {table_name}"
    query_output = execute_query(query_statement)
    print("Number of rows in the table:", query_output.iloc[0, 0])
