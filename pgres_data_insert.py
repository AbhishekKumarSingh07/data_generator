import psycopg2
from typing import List

class DataRepository:
    """
    Handles database operations for PostgreSQL.
    """

    def __init__(self, db_connector, table_name: str):
        self.db_connector = db_connector
        self.table_name = table_name

    def create_table(self, columns: List[str]):
        """
        Creates a table dynamically based on CSV headers.
        """
        column_definitions = ", ".join([f'"{col}" TEXT' for col in columns])
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id SERIAL PRIMARY KEY, 
            {column_definitions}
        )"""

        connection = self.db_connector.get_connection()
        if connection:
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()
            cursor.close()
            connection.close()

    def insert_data(self, data: List[dict]):
        """
        Inserts CSV data into the PostgreSQL database.
        """
        if not data:
            return

        columns = list(data[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join([f'"{col}"' for col in columns])
        query = f'INSERT INTO {self.table_name} ({column_names}) VALUES ({placeholders})'

        values = [tuple(row.values()) for row in data]

        connection = self.db_connector.get_connection()
        if connection:
            cursor = connection.cursor()
            cursor.executemany(query, values)
            connection.commit()
            cursor.close()
            connection.close()
