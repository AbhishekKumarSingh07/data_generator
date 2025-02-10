import os
import csv
import mysql.connector
from mysql.connector import Error
from typing import List


class DatabaseConnector:
    """
    Handles MySQL database connection.
    """

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            self.connection = None

    def get_connection(self):
        return self.connection

    def close(self):
        if self.connection:
            self.connection.close()


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
    Handles database operations.
    """

    def __init__(self, db_connector: DatabaseConnector, table_name: str):
        self.db_connector = db_connector
        self.table_name = table_name

    def create_table(self, columns: List[str]):
        """
        Creates a table dynamically based on CSV headers.
        """
        column_definitions = ", ".join([f"`{col}` TEXT" for col in columns])
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY, 
            {column_definitions}
        )"""

        connection = self.db_connector.get_connection()
        if connection:
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()
            cursor.close()

    def insert_data(self, data: List[dict]):
        """
        Inserts CSV data into the database.
        """
        if not data:
            return

        columns = list(data[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join([f'`{col}`' for col in columns])
        query = f"INSERT INTO {self.table_name} ({column_names}) VALUES ({placeholders})"

        values = [tuple(row.values()) for row in data]

        connection = self.db_connector.get_connection()
        if connection:
            cursor = connection.cursor()
            cursor.executemany(query, values)
            connection.commit()
            cursor.close()


if __name__ == "__main__":
    # Database configuration
    DB_CONFIG = {
        "host": "localhost",
        "user": "akki",
        "password": "Akki@test",
        "database": "conv_crm"
    }
    TABLE_NAME = "wikipedia"
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    #CSV_FILE_PATH = os.path.join(BASE_DIR, "data", "cricket", "batter_player_stats.csv")
    CSV_FILE_PATH = os.path.join(BASE_DIR, "data","people_wiki.csv")
    #CSV_FILE_PATH = "batter_player_stats.csv.csv"  # Update this with actual file path

    # Initialize components
    db_connector = DatabaseConnector(**DB_CONFIG)
    db_connector.connect()

    csv_data = CSVReader.read_csv(CSV_FILE_PATH)

    if csv_data:
        repository = DataRepository(db_connector, TABLE_NAME)
        repository.create_table(columns=list(csv_data[0].keys()))
        repository.insert_data(csv_data)

    db_connector.close()
