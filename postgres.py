import os
import csv
import psycopg2
from psycopg2 import OperationalError
import psycopg2
from typing import List


class CSVReader:
    """
    Reads CSV file and returns structured data.
    """

    @staticmethod
    def read_csv(file_path: str) -> List[dict]:
        try:
            with open(file_path, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                return [row for row in reader]
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return []

class DataRepository:
    """
    Handles database operations for PostgreSQL.
    """

    def __init__(self, db_connector, table_name: str):
        self.db_connector = db_connector
        self.table_name = table_name
        self.connection = self.db_connector.get_connection()


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

        #connection = self.db_connector.get_connection()
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
            cursor.close()

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

        #connection = self.db_connector.get_connection()
        if self.connection:
            cursor = self.connection.cursor()
            cursor.executemany(query, values)
            self.connection.commit()
            cursor.close()


class PostgresDbConnector:
    """
    Handles PostgreSQL database connection.
    """

    def __init__(self, host: str, user: str, password: str, database: str, port: int = 5432):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port
            )
            print("Connected to PostgreSQL database successfully.")
        except OperationalError as e:
            print(f"Error connecting to PostgreSQL: {e}")
            self.connection = None

    def get_connection(self):
        return self.connection

    def close(self):
        if self.connection:
            self.connection.close()
            print("PostgreSQL connection closed.")


if __name__ == "__main__":
    # Initialize the connector
    pg_connector = PostgresDbConnector(
        host="localhost",
        user="postgres",
        password="root",
        database="kaggle"
    )
    table_name = "bowler"
    # Connect to the database
    pg_connector.connect()


    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    CSV_FILE_PATH = os.path.join(BASE_DIR, "data", "cricket", "bowler_player_stats.csv")

    csv_data = CSVReader.read_csv(CSV_FILE_PATH)

    # Get the connection and use it
    if csv_data:
        repo = DataRepository(pg_connector, table_name)
        repo.create_table(columns=list(csv_data[0].keys()))
        repo.insert_data(csv_data)

    # Close the connection
    pg_connector.close()
