import os
from typing import Optional, List


def add_failed_database(db_name: str) -> None:
    """
    Adds a failed database name to the 'failed_databases.log' file if it is not already present.

    :param db_name: The name of the database that failed.
    :return: None
    """
    failed_databases = []

    # Check if the log file exists and read the current failed databases
    if os.path.exists('./failed_databases.log'):
        with open('failed_databases.log', 'r') as file:
            failed_databases = [line.strip() for line in file]

    # Add the database to the log file if it is not already listed
    if db_name not in failed_databases:
        with open('failed_databases.log', 'a') as file:
            file.write(f"{db_name}\n")


def remove_failed_databases() -> None:
    """
    Deletes the 'failed_databases.log' file, removing all records of failed databases.

    :return: None
    """
    if os.path.exists('./failed_databases.log'):
        os.remove('./failed_databases.log')


def exists_failed_databases() -> bool:
    """
    Checks if the 'failed_databases.log' file exists.

    :return: True if the log file exists, False otherwise.
    """
    return os.path.exists('./failed_databases.log')


def get_failed_dbs() -> Optional[List[str]]:
    """
    Retrieves the list of failed databases from the 'failed_databases.log' file.

    :return: A list of failed database names, or None if the file doesn't exist.
    """
    if not os.path.exists('./failed_databases.log'):
        return None

    with open('failed_databases.log', 'r') as file:
        return [line.strip() for line in file]
