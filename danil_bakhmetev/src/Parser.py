import csv
import logging

class Parser_Employee:
    """
    Класс для парсинга CSV файлов
    """
    def __init__(self, path):
        self.data = []
        self.parse_csv(path)

    def parse_csv(self, path: str) -> None:
        """Reads a CSV file into memory

        Args:
            path (str): Path to the file

        Raises:
            ValueError: If path is empty
            IOError: If there is an error reading the file
        """
        # Check if the path is empty
        if not path:
            raise ValueError("Path cannot be empty")

        try:
            # Read the file in one go
            with open(path, "r", newline="") as file:
                # Create a reader and read the file
                reader = csv.DictReader(file)
                # Convert to a list and assign to data
                self.data = [dict(row) for row in reader if self.check_row(row)]
        except IOError as e:
            # If there is an error reading the file, raise an IOError
            raise IOError(f"Error reading {path}: {e}") from e

    def check_row(self, row: dict) -> bool:
        """Check if the row has the correct structure

        Args:
            row (dict): A dictionary with the row

        Returns:
            bool: True if the row is valid, False if not
        """
        valid_types = {
            "E_ID": int,
            "E_NAME": lambda x: isinstance(x, str) and len(x) <= 30,
            "E_DESIGNATION": lambda x: isinstance(x, str) and len(x) <= 40,
            "E_ADDR": lambda x: isinstance(x, str) and len(x) <= 100,
            "E_BRANCH": lambda x: isinstance(x, str) and len(x) <= 15,
            "E_CONT_NO": int,
        }
        for key, value_type in valid_types.items():
            if not isinstance(row[key], value_type):
                logging.error(
                    f"Error: {key} should be {value_type.__name__}, not {type(row[key]).__name__}"
                )
                return False
        return True




s = Parser_Employee('./Employee_Details.csv')
print(s.data)