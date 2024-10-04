import decimal
import mysql.connector
import time as tm
import concurrent.futures
import threading
from time import sleep
from mysql.connector import errorcode
from tqdm import tqdm
from logs import log_message, LogType
from mysql.connector.cursor_cext import CMySQLCursorBuffered
from mysql.connector.connection_cext import CMySQLConnection
from progress import update_pbar, create_pbar, close_pbar, PbarColors, PbarPrompts
from config import databases_to_avoid, databases_to_migrate, sys_databases
from datetime import datetime, date, time
from colorama import Fore, Style, Back
from typing import List, Tuple, Dict
from failed import add_failed_database, exists_failed_databases, get_failed_dbs
from config import source_config, destination_config


# Create a lock to protect database operations
db_lock = threading.Lock()


def is_db_listed_as_migrable(db_name: str) -> bool:
    """
    Check if a database is listed as migrable.

    :param db_name: Name of the database to check.
    :return: True if the database is migrable, False otherwise.
    """
    # Must not be in sys_databases collection
    if db_name in sys_databases:
        return False

    # Must not be in databases_to_avoid collection
    if db_name in databases_to_avoid:
        return False

    # Is it in the databases_to_migrate list?
    if db_name in databases_to_migrate:
        return True

    # Migrate only if databases_to_migrate is empty
    return len(databases_to_migrate) == 0


def escape_value(value: object, columns: List[object], index: int, row: List[Tuple]) -> object:
    """
    Escape and format a database value for migration.

    :param value: The value from the database.
    :param columns: The column definitions of the table.
    :param index: The index of the column being processed.
    :param row: The row data from the database.
    :return: The formatted value.
    """
    # Get column type from the column definition
    column_type = columns[index][1]

    # Handle string values, including boolean strings
    if isinstance(value, str):
        if value.lower() == 'true':
            return 1
        elif value.lower() == 'false':
            return 0
        return value

    # Handle set values by joining them into a string
    elif isinstance(value, set):
        return ','.join(value)

    # Convert boolean values to integer representation
    elif isinstance(value, bool):
        return 1 if value else 0

    # Convert datetime, date, and time objects to ISO format
    elif isinstance(value, (datetime, date, time)):
        return value.isoformat()

    # Convert Decimal objects to strings
    elif isinstance(value, decimal.Decimal):
        return str(value)

    # Handle NULL values, with special treatment for date-related fields
    elif value is None:
        if column_type is mysql.connector.constants.FieldType.DATE:
            return datetime.strptime('0001-01-01', '%Y-%m-%d').isoformat()
        elif column_type is mysql.connector.constants.FieldType.DATETIME:
            return datetime.strptime('0001-01-01 00:00:00', '%Y-%m-%d %H:%M:%S').isoformat()
        elif column_type is mysql.connector.constants.FieldType.TIMESTAMP:
            return None

        return None

    # Return the value if no specific handling is required
    else:
        return value


def escape_column_name(column_name: str) -> str:
    """
    Escapes a MySQL column name by surrounding it with backticks.

    :param column_name: The column name to escape.
    :return: The escaped column name.
    """
    # Surround column name with backticks for MySQL syntax
    return f"`{column_name}`"


def get_database_schema(cursor: CMySQLCursorBuffered, db_name: str, tables: List[str]) -> bool:
    """
    Retrieve the schema of a MySQL database, including table, view, trigger, procedure, and function definitions.

    :param cursor: A buffered MySQL cursor to execute queries.
    :param db_name: The name of the database to retrieve schema from.
    :param tables: A tuple of specific tables to retrieve the schema for.
    :return: A dictionary with schema definitions for tables, views, triggers, procedures, and functions.
    """
    # Dictionary to store schema statements
    create_statements = {}

    # Switch to the specified database
    cursor.execute(f"USE `{db_name}`")

    # Get all table definitions if a list of tables is provided
    if tables:
        for table_name in tables:
            cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
            create_statements[table_name] = cursor.fetchone()[1] + ";"

    # Get all view definitions
    cursor.execute(f"SHOW FULL TABLES IN `{db_name}` WHERE TABLE_TYPE LIKE 'VIEW'")
    views = cursor.fetchall()
    for view in views:
        cursor.execute(f"SHOW CREATE VIEW `{view[0]}`")
        create_statements[view[0]] = cursor.fetchone()[1] + ";"

    # Get all trigger definitions
    cursor.execute("SHOW TRIGGERS")
    triggers = cursor.fetchall()
    for trigger in triggers:
        cursor.execute(f"SHOW CREATE TRIGGER `{trigger[0]}`")
        create_statements[trigger[0]] = cursor.fetchone()[2] + ";"

    # Get all procedure definitions
    cursor.execute(f"SHOW PROCEDURE STATUS WHERE Db = '{db_name}'")
    procedures = cursor.fetchall()
    for procedure in procedures:
        cursor.execute(f"SHOW CREATE PROCEDURE `{procedure[1]}`")
        create_statements[procedure[1]] = cursor.fetchone()[2] + ";"

    # Get all function definitions
    cursor.execute(f"SHOW FUNCTION STATUS WHERE Db = '{db_name}'")
    functions = cursor.fetchall()
    for function in functions:
        cursor.execute(f"SHOW CREATE FUNCTION `{function[1]}`")
        create_statements[function[1]] = cursor.fetchone()[2] + ";"

    # Return the dictionary containing the schema definitions
    return create_statements


def change_keys_status(cursor: CMySQLCursorBuffered, enabled: bool = False) -> None:
    """
    Enable or disable the foreign key checks in MySQL.

    :param cursor: A buffered MySQL cursor to execute the query.
    :param enabled: If True, enables foreign key checks. If False, disables them.
    :return: None
    """
    # Disable or enable unique and foreign key checks based on the 'enabled' parameter
    cursor.execute(f"SET UNIQUE_CHECKS = {1 if enabled else 0}")
    cursor.execute(f"SET FOREIGN_KEY_CHECKS = {1 if enabled else 0}")


def migrate_schema(db_name: str) -> List[str]:
    """
    Migrates the schema of a specified database from the source to the destination.

    :param db_name: The name of the database to migrate.
    :return: A list of table names that were migrated.
    :raises: Raises an exception if the migration fails.
    """
    try:
        # Establish connections to source and destination databases
        src_cur, dst_cur, src_conn, dst_conn = connect(set_session_vars=False, src_db=db_name)

        # Disable foreign key checks on destination database
        change_keys_status(cursor=dst_cur, enabled=False)

        # Create the database on the destination server
        dst_cur.execute(f"CREATE DATABASE `{db_name}`")
        dst_cur.execute(f"USE `{db_name}`")

        # Get all tables in correct creation order
        tables = get_all_tables(databases=[db_name], cursor=src_cur)

        # Get creation SQL statements for all objects
        create_statements = get_database_schema(cursor=src_cur, db_name=db_name, tables=tables)

        # Create each table in the destination database
        for table in tables:
            try:
                dst_cur.execute(create_statements[table])
            except mysql.connector.Error as ex:
                if ex.errno != errorcode.ER_TABLE_EXISTS_ERROR:
                    raise ex

        # Re-enable foreign key checks on destination database
        change_keys_status(cursor=dst_cur, enabled=True)

        # Close all database connections and cursors
        close_handlers(src_cur, dst_cur, src_conn, dst_conn)

        # Return the list of migrated tables
        return tables

    except Exception as ex:
        # Log critical error and handle failure
        log_message(f"Error found during target database schema creation for database `{db_name}`! {ex}", LogType.CRITICAL)

        # Add the failed database to the log
        add_failed_database(db_name)

        # Remove the failed database from the destination server
        remove_database(db_name)

        # Re-raise the exception for further handling
        raise ex


def migrate_procedures(db_name: str) -> bool:
    """
    Migrates stored procedures, functions, and triggers from the source database to the destination.

    :param db_name: The name of the database to migrate procedures from.
    :return: True if successful, False if an error occurs.
    """
    try:
        # Establish connections to source and destination databases
        src_cur, dst_cur, src_conn, dst_conn = connect(set_session_vars=True, src_db=db_name)

        # Select database
        dst_cur.execute(f"USE `{db_name}`")

        # Disable foreign key checks on destination database
        change_keys_status(cursor=dst_cur, enabled=False)

        # Get creation SQL statements for procedures, functions, and triggers
        create_statements = get_database_schema(src_cur, db_name, None)

        # Execute each statement related to triggers, procedures, and functions
        for _, statement in create_statements.items():
            if any(keyword in statement for keyword in ["TRIGGER", "PROCEDURE", "FUNCTION"]):
                try:
                    dst_cur.execute(statement)
                except mysql.connector.Error as ex:
                    if ex.errno != errorcode.ER_TABLE_EXISTS_ERROR:
                        raise ex

        # Re-enable foreign key checks on destination database
        change_keys_status(cursor=dst_cur, enabled=True)

        # Close all database connections and cursors
        close_handlers(src_cur, dst_cur, src_conn, dst_conn)

        # Return True to indicate success
        return True

    except Exception as err:
        # Log critical error and handle failure
        log_message(f"Error found during target database procedures creation for database `{db_name}`! {err}", LogType.CRITICAL)

        # Add the failed database to the log
        add_failed_database(db_name)

        # Remove the failed database from the destination server
        remove_database(db_name)

        # Return False to indicate failure
        return False


def migrate_database(db_name: str, batch_size: int = 4096) -> None:
    """
    Migrates an entire database including schema, tables, and procedures.

    :param db_name: The name of the database to migrate.
    :param batch_size: The batch size for migrating table data. Defaults to 4096.
    :return: None
    """
    try:
        # Recreate the schema of the database
        tables = migrate_schema(db_name=db_name)

        # Migrate data of the tables in the database
        migrate_database_tables(db_name=db_name, tables=tables, batch_size=batch_size)

        # Migrate stored procedures, triggers, functions, etc.
        migrate_procedures(db_name=db_name)
    except Exception as err:
        log_message(f"Error found during migration of database `{db_name}`! {err}", LogType.CRITICAL)

        # Store failed database information
        add_failed_database(db_name=db_name)

        # Remove the database since the process failed
        remove_database(db_name=db_name)

        # Raise the error to allow further handling
        raise err


def migrate_database_tables(db_name: str, tables: List[str], batch_size: int) -> None:
    """
    Migrates the table data of a database in batches.

    :param db_name: The name of the database.
    :param tables: A tuple of table names to migrate.
    :param batch_size: The number of records to process in each batch.
    :return: None
    """
    # Get the number of tables to migrate
    table_count = len(tables)

    # If no tables are found, nothing needs to be migrated
    if table_count == 0:
        return

    # Establish connections to source and destination databases
    src_cur, dst_cur, src_conn, dst_conn = connect(set_session_vars=True, src_db=db_name, dst_db=db_name)

    # Disable foreign key checks on the destination
    change_keys_status(cursor=dst_cur, enabled=False)

    # Disable keys for all tables before migration
    for table in tables:
        dst_cur.execute(f"ALTER TABLE `{table}` DISABLE KEYS")

    # Start a new transaction with READ UNCOMMITTED isolation level
    dst_conn.start_transaction(isolation_level='READ UNCOMMITTED', readonly=False)

    # Flag to track process success
    process_success = True

    # Process tables in groups of 4
    with concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix='mysql-migrator-table-thread') as executor:
        futures = []

        # Submit each database migration as a separate thread
        for table_name in tables:
            # Push task to the thread pool
            futures.append(executor.submit(lambda src_cur=src_cur, dst_cur=dst_cur, src_conn=src_conn, dst_conn=dst_conn, db_name=db_name, table_name=table_name, original_batch_size=batch_size:
                           (db_name, table_name, src_cur, dst_cur, migrate_table_data(src_cur=src_cur, dst_cur=dst_cur, src_conn=src_conn, dst_conn=dst_conn, db_name=db_name, table_name=table_name, original_batch_size=original_batch_size))))

        # Wait for all threads to complete
        for future in concurrent.futures.as_completed(futures):
            # Wait for
            db_name, table_name, src_cur, dst_cur, (result, ex) = future.result()

            if not result or not migration_success(db_name=db_name, table_name=table_name, src_cur=src_cur, dst_cur=dst_cur):
                # Mark the process as unsuccessful
                process_success = False

                # Rollback the transaction
                dst_conn.rollback()

                # Raise an exception indicating failure
                raise Exception(f"Failed to migrate table `{table}` for database `{db_name}`: {ex}")

    # If the process succeeded, commit the transaction
    if process_success:
        dst_conn.commit()

    # Re-enable keys for all tables after migration
    for table in tables:
        dst_cur.execute(f"ALTER TABLE `{table}` ENABLE KEYS")

    # Re-enable foreign key checks on the destination
    change_keys_status(cursor=dst_cur, enabled=True)

    # Close all database connections and cursors
    close_handlers(src_cur=src_cur, dst_cur=dst_cur, src_conn=src_conn, dst_conn=dst_conn)


def migrate_table_data(
    src_cur: CMySQLCursorBuffered,
    dst_cur: CMySQLCursorBuffered,
    src_conn: CMySQLConnection,
    dst_conn: CMySQLConnection,
    db_name: str,
    table_name: str,
    original_batch_size: int
) -> Tuple[bool, Exception]:
    """
    Migrates data from a source table to a destination table, handling errors, progress, and adjusting batch sizes.

    :param src_cur: Source database cursor.
    :param dst_cur: Destination database cursor.
    :param src_conn: Source database connection.
    :param dst_conn: Destination database connection.
    :param db_name: Name of the database.
    :param table_name: Name of the table to migrate.
    :param original_batch_size: Initial batch size for row migration.

    :return: A tuple with the migration success status and any encountered exception.
    """
    # Get PK info
    with db_lock:
        pk = get_table_pk(db_name, table_name, src_cur)
        pk_count = get_table_pk_count(db_name, table_name, src_cur)

        # Get PK count (may be *)
        row_count_query = f"SELECT COUNT({pk}) FROM `{table_name}`"
        src_cur.execute(row_count_query)
        row_count = src_cur.fetchone()[0]

        # Return if 0 rows
        if row_count == 0:
            return True, None

        # Get first PK value
        first_pk = 0
        if pk != '*' and pk_count == 1:
            src_cur.execute(f"SELECT {pk} FROM `{table_name}` ORDER BY {pk} ASC LIMIT 1")
            first_pk = src_cur.fetchone()[0]

        # Get table columns
        src_cur.execute(f"SELECT * FROM `{table_name}` LIMIT 1")
        rows = src_cur.fetchone()[0]

        # Get tables description
        column_names = [escape_column_name(i[0]) for i in src_cur.description]
        columns = src_cur.description

        # Generate a good batch_size
        batch_size = original_batch_size
        has_long_columns = False

        # Set primary key index if there is a valid primary key and it is a single-column PK
        if pk != '*' and pk_count == 1:
            pk_index = column_names.index(pk)

            # Define a set of valid numerical primary key types for clarity
            valid_pk_types = {
                mysql.connector.constants.FieldType.BIT,
                mysql.connector.constants.FieldType.DOUBLE,
                mysql.connector.constants.FieldType.ENUM,
                mysql.connector.constants.FieldType.FLOAT,
                mysql.connector.constants.FieldType.INT24,
                mysql.connector.constants.FieldType.LONG,
                mysql.connector.constants.FieldType.LONGLONG,
                mysql.connector.constants.FieldType.SHORT,
                mysql.connector.constants.FieldType.TINY,
                mysql.connector.constants.FieldType.YEAR
            }

            # If the primary key is not one of the valid types, mark it as '*'
            if columns[pk_index][1] not in valid_pk_types:
                pk = '*'
        else:
            pk_index = 0

    # Check if any column is of type MEDIUM_BLOB or LONG_BLOB, which may require batch size adjustments
    blob_types = {
        mysql.connector.constants.FieldType.MEDIUM_BLOB,
        mysql.connector.constants.FieldType.LONG_BLOB
    }

    # Use any() to efficiently check for BLOB columns
    has_long_columns = any(col[1] in blob_types for col in columns)
    if has_long_columns:
        batch_size = batch_size // 5

    # Some defaults
    calculated_batch_size = batch_size
    increment_batch = 0
    increment_batch_blocks = 0
    boost = 2048

    # Create and update progress
    progress_created = False

    # Iterar sobre la tabla en bloques de tamaño batch_size
    offset = 0
    while offset < row_count:
        # Get current tyme
        now = tm.time()

        try:
            # Run SELECT statement
            with db_lock:
                if pk == '*' or pk_count != 1:
                    src_cur.execute(f"SELECT SQL_NO_CACHE * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}")
                else:
                    src_cur.execute(f"SELECT SQL_NO_CACHE * FROM `{table_name}` WHERE {pk} >= {first_pk} ORDER BY {pk} ASC LIMIT {batch_size}")

                # Get rows
                rows = src_cur.fetchall()

            # No more rows? Quit
            if not rows:
                break

            # Is first time? Then create progress bar
            if not progress_created:
                progress_created = True
                progress = create_pbar(row_count, leave=False, colour=PbarColors.TABLE, units='row')
                update_pbar(progress=progress, number=0, message=f"`{db_name}`.`{table_name}` reading...", prompt=PbarPrompts.PERCENT_PROMPT)

            # Get next value
            if pk != '*' and pk_count == 1:
                first_pk = rows[len(rows) - 1][pk_index] + 1

            # Generate placeholders
            values_placeholder = ', '.join(['%s'] * len(column_names))

            # Generate INSERT query
            insert_query = f"INSERT INTO {escape_column_name(table_name)} ({', '.join(column_names)}) VALUES ({values_placeholder})"

            # Insert rows
            with db_lock:
                # Resolve batch
                batch_resolved = [tuple(escape_value(value, columns, index, row) for index, value in enumerate(row)) for row in rows]

                # Execute
                dst_cur.executemany(insert_query, batch_resolved)

            # Increment countes
            increment_batch = increment_batch + 1

            # Time to boost?
            if increment_batch == 10:
                increment_batch_blocks = increment_batch_blocks + 1
                batch_size = min(calculated_batch_size + (increment_batch_blocks * boost), batch_size * 6)
                increment_batch = 0
        except mysql.connector.Error as ex:
            # 1062 = Primary key already exists; 1064 (42000): You have an error in your SQL syntax;
            if ex.errno == 1062 or ex.errno == 1064:
                try:
                    on_error_insert_single(rows, insert_query, table_name, db_name, columns, dst_cur, progress)
                except Exception as ex:
                    return False, ex
            # 2013 Connection lost while querying
            elif ex.errno == 2013:
                try:
                    # Reconnect
                    src_conn, dst_conn = reconnect_to_db(src_conn, dst_conn, src_cur, dst_cur, db_name)

                    # Resize batch
                    batch_size = max(1, batch_size // 8)

                    # Update progress
                    batch_info = f" [{Fore.YELLOW}{batch_size}{Style.RESET_ALL} rows/batch]"
                    update_pbar(progress=progress, colour=PbarColors.THROTTLED, number=0, message=f"[{Fore.RED}throttled{Style.RESET_ALL}] `{db_name}`.`{table_name}`{batch_info}", prompt=PbarPrompts.PERCENT_PROMPT)

                    # Repeat batch
                    continue
                except Exception as ex:
                    return False, ex
            else:
                return False, ex
        except Exception as ex:
            return False, ex

        # Increment offset
        offset += len(rows)

        # Pick time
        later = tm.time()

        # Calculate difference
        difference = round((later - now), 2)

        # If it takes more than 4 seconds to execute, we reduce the batch size and alert (they were 16 and 20) (they were 4 and 8)
        difference_info = ''
        difference_color = Fore.WHITE
        if difference > 4:
            # Set prompt color
            difference_color = Fore.RED

            # Change batch size
            batch_size = max(1, batch_size // 2)

            # Reset counters
            increment_batch = 0
            increment_batch_blocks = 0

            # Calculate diff time
            diff = min(difference // 4, 60)

            # Update progress
            update_pbar(progress=progress, colour="#cc745e", number=0, message=f"`{db_name}`.`{table_name}` {Fore.YELLOW}going to sleep for {diff}s{Style.RESET_ALL}", prompt=PbarPrompts.PERCENT_PROMPT)

            # Sleep diff time
            tm.sleep(diff)
        elif difference > 2:
            # Set prompt color
            difference_color = Fore.YELLOW

            # Substract from counters
            increment_batch = max(0, increment_batch - 1)
            increment_batch_blocks = max(0, increment_batch_blocks - 1)

            # Change batch size
            batch_size = max(1, batch_size // 2)
        else:
            difference_color = Fore.GREEN

        # Generate differente info text
        difference_info = f'[{difference_color}{difference:.2f}s{Style.RESET_ALL} last batch]'

        # Get prompt properties
        colour, prompt = generate_progress_prompts(
            batch_size=batch_size,
            calculated_batch_size=calculated_batch_size,
            pk=pk,
            pk_count=pk_count,
            db_name=db_name,
            table_name=table_name,
            difference_info=difference_info
        )

        # Update progress bar
        update_pbar(progress=progress, number=len(rows), colour=colour, message=prompt, prompt=PbarPrompts.PERCENT_PROMPT)

        # Finished? Don't refresh or sleep thread
        if offset >= row_count:
            break

        # Refresh and give some time
        progress.refresh()
        sleep(0.1)

    # Close progress
    close_pbar(progress)
    sleep(0.1)

    # Return success
    return True, None


def generate_progress_prompts(
    batch_size: int,
    calculated_batch_size: int,
    pk: int,
    pk_count: int,
    db_name: str,
    table_name: str,
    difference_info: str
) -> Tuple[str, str]:
    """
    Generates progress prompt information for display during data migration.
    The function builds the message for the progress bar, including batch size,
    primary key information, and throttling status.

    :param batch_size: The current batch size being used for data migration.
    :param calculated_batch_size: The initial calculated batch size before adjustments.
    :param pk: The primary key column for the table (or '*' if none).
    :param pk_count: The number of primary key columns in the table.
    :param db_name: The name of the database being processed.
    :param table_name: The name of the table being migrated.
    :param difference_info: A string representing the time difference info for the last batch.
    :return: A tuple containing the color for the progress bar and the message to display in the progress bar.
    """
    batch_info = ''
    throttled_info = ''
    pk_msg = ''
    colour = '#ffffff'  # Default color for the progress bar

    # Adjust progress message and color based on batch size
    if batch_size < calculated_batch_size:
        # Batch size reduced (throttled), display in yellow/red
        batch_info = f"[{Fore.YELLOW}{batch_size}{Style.RESET_ALL} rows/batch] {difference_info}"
        throttled_info = f'({Fore.RED}throttled{Style.RESET_ALL}) '
        colour = '#cc745e'  # Color indicating throttling
    else:
        colour = '#cc995e'  # Normal batch size color
        if batch_size > calculated_batch_size:
            # Batch size increased due to boost, display in green
            batch_info = f"[{Fore.GREEN}{batch_size}{Style.RESET_ALL} rows/batch -{Fore.BLACK}{Back.LIGHTGREEN_EX}boost{Style.RESET_ALL}-] {difference_info}"
        else:
            # Normal batch size, display in green
            batch_info = f"[{Fore.GREEN}{batch_size}{Style.RESET_ALL} rows/batch] {difference_info}"

    # Add primary key information to the progress message
    if pk != '*' and pk_count == 1:
        # If there's a single primary key, display it in green
        pk_msg = f' [{Fore.GREEN}using {pk.replace("`", "")} pk{Style.RESET_ALL}]'
    else:
        if pk_count > 1:
            # Multiple primary keys, display the count in yellow
            pk_msg = f' [{Fore.YELLOW}{pk_count} pks{Style.RESET_ALL}]'
        else:
            # No primary key, display an error in red
            pk_msg = f' [{Fore.RED}no pk{Style.RESET_ALL}]'

    # Return the color and the full progress message
    return colour, f"{throttled_info}`{db_name}`.`{table_name}`{pk_msg}{batch_info}"


def on_error_insert_single(batch, insert_query: str, table_name: str, db_name: str, columns: str, dst_cur: CMySQLCursorBuffered, progress) -> None:
    """
    Executes insert statements for each row in the batch one by one, typically after a batch failure.

    :param batch: The batch of rows to insert.
    :param insert_query: The SQL insert query to execute.
    :param table_name: The name of the table where data is being inserted.
    :param db_name: The name of the database containing the table.
    :param columns: The list of columns involved in the insert.
    :param dst_cur: The cursor for the destination database.
    :param progress: A progress bar object to display progress.
    :return: None
    """
    # Update progress bar to indicate the start of single row batch execution
    progress.set_description(f"[{Fore.CYAN}%{Style.RESET_ALL}] "
                             f"[{Fore.RED}throttled{Style.RESET_ALL}] "
                             f"`{db_name}`.`{table_name}` {Fore.RED}performing 1 row batches{Style.RESET_ALL}")

    # Iterate over each row in the batch and insert individually
    for row in batch:
        try:
            # Prepare each statement by escaping and formatting values
            statement = tuple(escape_value(value=value, columns=columns, index=index, row=row)
                              for index, value in enumerate(row))
            with db_lock:
                # Execute the insert query with the current row data
                dst_cur.execute(insert_query, statement)
        except mysql.connector.Error as ex:
            # If there's a duplicate entry error (1062), continue to the next row
            if ex.errno == 1062:
                continue
            else:
                # Raise any other errors
                raise ex


def migrate_grants(batch_size: int = 4096) -> None:
    """
    Migrates the grants (privileges) from the source MySQL database to the destination.

    :param batch_size: The batch size for migrating table data. Defaults to 4096.
    :return: None
    """
    try:
        # Connect to the source and destination MySQL databases
        src_cur, dst_cur, src_conn, dst_conn = connect(set_session_vars=True, src_db='mysql')

        # Begin a new transaction in the destination database
        dst_conn.start_transaction()

        # Migrate the user table from the MySQL database (contains grants)
        migrate_table_data(src_cur=src_cur, dst_cur=dst_cur, src_conn=src_conn, dst_conn=dst_conn,
                           db_name="mysql", table_name="user", original_batch_size=batch_size)

        # Commit the transaction after successful migration
        dst_conn.commit()

        # Close all database connections and cursors
        close_handlers(src_cur=src_cur, dst_cur=dst_cur, src_conn=src_conn, dst_conn=dst_conn)

        # Log a success message
        log_message("Grants migrated", LogType.ADD)
    except Exception as err:
        # Rollback the transaction in case of any error
        dst_conn.rollback()
        log_message(f"Error migrating GRANTS: {err}", LogType.ERROR)


def get_all_tables(databases: List[str], cursor: CMySQLCursorBuffered, verbose: bool = False, skipped_databases: List[str] = None) -> List[str]:
    """
    Retrieves all tables from the provided databases.

    :param databases: A list of database names to scan for tables.
    :param cursor: A MySQL cursor to execute the queries.
    :param verbose: Whether to display progress of the scanning process. Defaults to False.
    :param skipped_databases: A list of databases to skip during the scanning process. Defaults to None.
    :return: A list of table names found across the databases.
    """
    tables = []

    # If only one database is provided, scan it for tables
    if len(databases) == 1:
        if not skipped_databases or databases[0] not in skipped_databases:
            cursor.execute(f"USE `{databases[0]}`")
            cursor.execute(f"SHOW FULL TABLES IN `{databases[0]}` WHERE table_type LIKE 'BASE TABLE'")
            tables += [table[0] for table in cursor.fetchall()]
    else:
        # Initialize progress bar if verbose is enabled
        progress = tqdm(total=len(databases), leave=False, colour="#cc33ba", unit='table') if verbose else None

        # Iterate over the list of databases
        for db_name in databases:
            if verbose:
                progress.set_description(f"[{Fore.CYAN}%{Style.RESET_ALL}] Scanning tables for database `{db_name}`. {len(tables)} found so far")
                progress.update(1)

            # Check if the database is listed as migrable and not in the skipped list
            if is_db_listed_as_migrable(db_name) and (not skipped_databases or db_name not in skipped_databases):
                cursor.execute(f"USE `{db_name}`")
                cursor.execute(f"SHOW FULL TABLES IN `{db_name}` WHERE table_type LIKE 'BASE TABLE'")
                tables += [table[0] for table in cursor.fetchall()]

        # Close the progress bar if it was used
        if progress:
            close_pbar(progress)

    return tables


def get_process_dbs(cursor: CMySQLCursorBuffered) -> List[str]:
    """
    Retrieves the list of databases to be processed from the MySQL server.

    :param cursor: A MySQL cursor to execute the database query.
    :return: A list of database names that are marked as migrable.
    """
    cursor.execute("SHOW DATABASES")
    dbs = [db[0] for db in cursor.fetchall() if is_db_listed_as_migrable(db[0])]
    return dbs


def close_handlers(src_cur: CMySQLCursorBuffered, dst_cur: CMySQLCursorBuffered, src_conn: CMySQLConnection, dst_conn: CMySQLConnection) -> None:
    """
    Closes the provided source and destination cursors and connections.

    :param src_cur: The cursor for the source database.
    :param dst_cur: The cursor for the destination database.
    :param src_conn: The connection for the source database.
    :param dst_conn: The connection for the destination database.
    :return: None
    """
    if src_cur:
        src_cur.close()
    if dst_cur:
        dst_cur.close()
    if src_conn:
        src_conn.close()
    if dst_conn:
        dst_conn.close()


def reconfigure_db_session(src_cur: CMySQLCursorBuffered, dst_cur: CMySQLCursorBuffered) -> None:
    """
    Reconfigures the session variables for both source and destination database cursors.

    :param src_cur: The cursor for the source database.
    :param dst_cur: The cursor for the destination database.
    :return: None
    """
    if src_cur:
        # Set session timeouts and limits for source database
        src_cur.execute("SET SESSION WAIT_TIMEOUT = 14400")
        src_cur.execute("SET SESSION MAX_EXECUTION_TIME = 7200000")  # 2 hours in milliseconds
        src_cur.execute("SET SESSION net_read_timeout = 14400")
        src_cur.execute("SET SESSION net_write_timeout = 14400")
        src_cur.execute("SET SESSION interactive_timeout = 14400")

    if dst_cur:
        # Set session timeouts and limits for destination database
        dst_cur.execute("SET SESSION WAIT_TIMEOUT = 14400")
        dst_cur.execute("SET SESSION MAX_EXECUTION_TIME = 7200000")  # 2 hours in milliseconds
        dst_cur.execute("SET SESSION net_read_timeout = 14400")
        dst_cur.execute("SET SESSION net_write_timeout = 14400")
        dst_cur.execute("SET SESSION interactive_timeout = 14400")


def reconnect_to_db(src_conn: CMySQLConnection, dst_conn: CMySQLConnection,
                    src_cur: CMySQLCursorBuffered, dst_cur: CMySQLCursorBuffered,
                    src_db: str = None, dst_db: str = None) -> Tuple[CMySQLConnection, CMySQLConnection]:
    """
    Reconnects to both source and destination databases and reconfigures session variables.

    :param src_conn: The connection object for the source database.
    :param dst_conn: The connection object for the destination database.
    :param src_cur: The cursor for the source database.
    :param dst_cur: The cursor for the destination database.
    :param src_db: The default source database to use after reconnection, if provided.
    :param dst_db: The default destination database to use after reconnection, if provided.
    :return: A tuple containing the reconnected source and destination connection objects.
    """
    with db_lock:
        # Attempt to reconnect to both databases with retries
        src_conn.reconnect(attempts=3, delay=5)
        dst_conn.reconnect(attempts=3, delay=5)

        # Set destination connection to disable autocommit
        dst_conn.autocommit = False

        # Reconfigure session variables for both cursors
        reconfigure_db_session(src_cur=src_cur, dst_cur=dst_cur)

        # Use default databases if provided
        if src_db:
            src_cur.execute(f"USE `{src_db}`")
        if dst_db:
            dst_cur.execute(f"USE `{dst_db}`")

    # Return the reconnected connection objects
    return src_conn, dst_conn


def connect(set_session_vars: bool = True, src_db: str = None, dst_db: str = None) -> Tuple[CMySQLCursorBuffered, CMySQLCursorBuffered, CMySQLConnection, CMySQLConnection]:
    """
    Establishes connections to both source and destination databases and returns the corresponding cursors and connections.

    :param set_session_vars: Whether to set session variables for the connections.
    :param src_db: The source database to connect to, if provided.
    :param dst_db: The destination database to connect to, if provided.
    :return: A tuple containing source cursor, destination cursor, source connection, and destination connection.
    """
    # Establish connections to source and destination databases
    src_conn = mysql.connector.connect(**source_config)
    dst_conn = mysql.connector.connect(**destination_config)

    # Create cursors for both connections
    src_cur = src_conn.cursor(buffered=True)
    dst_cur = dst_conn.cursor(buffered=True)

    # Disable autocommit for the destination connection
    dst_conn.autocommit = False

    # Use default databases if provided
    if src_db:
        src_cur.execute(f"USE `{src_db}`")
    if dst_db:
        dst_cur.execute(f"USE `{dst_db}`")

    # Set session variables if required
    if set_session_vars:
        reconfigure_db_session(src_cur=src_cur, dst_cur=dst_cur)

    # Return the cursors and connections
    return src_cur, dst_cur, src_conn, dst_conn


def get_table_pk(database_name: str, table_name: str, cursor: CMySQLCursorBuffered) -> str:
    """
    Retrieves the primary key column name of a specified table in a database.

    :param database_name: The name of the database containing the table.
    :param table_name: The name of the table to retrieve the primary key from.
    :param cursor: A buffered MySQL cursor to execute the query.
    :return: The primary key column name enclosed in backticks, or '*' if no primary key exists.
    """
    query = (
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_KEY = 'PRI'"
    )
    cursor.execute(query, (database_name, table_name))
    primary_keys = cursor.fetchall()

    if primary_keys:
        # Return the first primary key column name with backticks
        return f"`{primary_keys[0][0]}`"
    else:
        # Return '*' if no primary key exists
        return "*"


def get_table_pk_count(database_name: str, table_name: str, cursor: CMySQLCursorBuffered) -> int:
    """
    Counts the number of primary key columns in a specified table within a database.

    :param database_name: The name of the database containing the table.
    :param table_name: The name of the table to count primary keys from.
    :param cursor: A buffered MySQL cursor to execute the query.
    :return: The number of primary key columns in the table.
    """
    query = (
        "SELECT COUNT(*) FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_KEY = 'PRI'"
    )
    cursor.execute(query, (database_name, table_name))
    count = cursor.fetchone()[0]
    return count


def migration_success(db_name: str, table_name: str, src_cur: CMySQLCursorBuffered, dst_cur: CMySQLCursorBuffered) -> bool:
    """
    Checks whether the migration of a table was successful by comparing the row counts
    between the source and destination databases.

    :param db_name: The name of the database.
    :param table_name: The name of the table to check.
    :param src_cur: The source database cursor.
    :param dst_cur: The destination database cursor.
    :return: True if the row counts match between the source and destination, False otherwise.
    """
    with db_lock:
        try:
            src_sizes = {}
            dst_sizes = {}

            # Count rows in the source table
            try:
                src_cur.execute(f"USE `{db_name}`")
                src_cur.execute(f"SELECT COUNT({get_table_pk(db_name, table_name, src_cur)}) FROM `{table_name}`")
                src_sizes[f"{db_name}.{table_name}"] = src_cur.fetchone()[0]
            except Exception as ex:
                log_message(f"Error counting rows in source table `{table_name}`: {ex}", LogType.ERROR)
                return False

            # Count rows in the destination table
            try:
                dst_cur.execute(f"USE `{db_name}`")
                dst_cur.execute(f"SELECT COUNT({get_table_pk(db_name, table_name, dst_cur)}) FROM `{table_name}`")
                dst_sizes[f"{db_name}.{table_name}"] = dst_cur.fetchone()[0]
            except Exception as ex:
                log_message(f"Error counting rows in destination table `{table_name}`: {ex}", LogType.ERROR)
                return False

            # Compare row counts between source and destination
            return src_sizes == dst_sizes

        except mysql.connector.Error as ex:
            log_message(f"Error checking migration status: {ex}", LogType.ERROR)
            return False
        except Exception as ex:
            log_message(f"Error checking migration status: {ex}", LogType.ERROR)
            return False


def count_migration_rows(src_dbs: List[str]) -> int:
    """
    Counts the total number of rows across all tables in the provided source databases.

    :param src_dbs: A list of source databases to count rows from.
    :return: The total number of rows counted across all databases.
    """
    try:
        # Connect to source and destination databases
        src_cur, dst_cur, src_conn, dst_conn = connect(set_session_vars=True)
        row_count = 0

        # Progress bar for tracking progress through databases
        progress = tqdm(total=len(src_dbs), leave=False, colour="#cc1c91", unit='database')

        # Loop through each source database
        for database in src_dbs:
            progress.update(1)
            try:
                src_cur.execute(f"USE `{database}`")
                src_tbls = get_all_tables([database], src_cur)

                # Count rows for each table in the current database
                for table in src_tbls:
                    progress.set_description(f"[{Fore.BLUE}%{Style.RESET_ALL}] Counting rows in database `{database}`...")
                    src_cur.execute(f"SELECT COUNT({get_table_pk(database, table, src_cur)}) FROM `{table}`")
                    row_count += src_cur.fetchone()[0]
            except Exception as ex:
                log_message(f"Error counting rows in database `{database}`: {ex}", LogType.WARNING)
                continue

        # Close the progress bar and all database handlers
        close_pbar(progress)
        close_handlers(src_cur, dst_cur, src_conn, dst_conn)

    except mysql.connector.Error as ex:
        log_message(f"Error counting migration rows: {ex}", LogType.ERROR)
    except Exception as ex:
        log_message(f"Error counting migration rows: {ex}", LogType.ERROR)

    return row_count


def check_process(skip_dbs: bool = False, only_check: bool = False, processed_dbs: List[str] = None) -> None:
    """
    Runs a check to verify if the migration process was successful. If in check-only mode,
    no updates will be made, only verification.

    :param skip_dbs: If True, skip certain databases during the check.
    :param only_check: If True, only checks the migration status without performing updates.
    :param processed_dbs: List of processed databases by migration process
    :return: None
    """
    if only_check:
        log_message(
            "Only Check mode found. No information will be written, updated, or deleted from the target instance.",
            LogType.WARNING
        )

    log_message("Running migration check...", LogType.INFO)

    # Check for failed databases and handle grants migration
    if exists_failed_databases():
        handle_failed_databases()

    # Call the function to show migration results
    show_results(skip_dbs=skip_dbs, processed_dbs=processed_dbs)


def show_results(skip_dbs: bool = False, processed_dbs: List[str] = None) -> None:
    """
    Compares row counts between source and destination databases to verify migration success.
    It generates progress bars and logs the status of each database and table.

    :param skip_dbs: Whether to skip certain databases during the comparison.
    :param processed_dbs: List of processed databases during migration process
    :return: None
    """
    src_sizes: Dict[str, int] = {}
    dst_sizes: Dict[str, int] = {}
    row_count = 0

    try:
        # Connect to both source and destination databases
        src_cur, dst_cur, src_conn, dst_conn = connect(set_session_vars=True)

        # Get the list of databases from the source
        src_dbs = processed_dbs if processed_dbs else get_process_dbs(cursor=src_cur)
        dst_dbs = processed_dbs if processed_dbs else get_process_dbs(cursor=dst_cur)
        failed_dbs = get_failed_dbs()  # Retrieve the list of failed databases

        # Check if there are databases to process
        if len(src_dbs) + len(dst_dbs) > 0:
            # Initialize a progress bar
            progress = create_pbar(len(src_dbs) + len(dst_dbs), leave=False, colour=PbarColors.INFO, units="database")

            # Check row counts in source databases
            for db in src_dbs:
                update_pbar(progress=progress, number=1, message=f"Checking [Source].`{db}`", prompt=PbarPrompts.PERCENT_PROMPT)
                dst_cur.execute(f"USE `{db}`")
                try:
                    # Get tables and row counts from the source database
                    tbls = get_all_tables([db], src_cur)
                    for table in tbls:
                        src_cur.execute(f"SELECT COUNT({get_table_pk(db, table, src_cur)}) FROM `{table}`")
                        src_sizes[f"{db}.{table}"] = src_cur.fetchone()[0]
                except (mysql.connector.Error, Exception) as ex:
                    log_message(f"Error checking rows in source database `{db}`: {ex}", LogType.ERROR)
                    close_pbar(progress)
                    raise ex

            # Check row counts in destination databases
            for db in dst_dbs:
                update_pbar(progress=progress, number=1, message=f"Checking [Destination].`{db}`", prompt=PbarPrompts.PERCENT_PROMPT)
                dst_cur.execute(f"USE `{db}`")
                try:
                    # Get tables and row counts from the destination database
                    tbls = get_all_tables([db], dst_cur)
                    for table in tbls:
                        dst_cur.execute(f"SELECT COUNT({get_table_pk(db, table, dst_cur)}) FROM `{table}`")
                        dst_sizes[f"{db}.{table}"] = dst_cur.fetchone()[0]
                        row_count += dst_sizes[f"{db}.{table}"]
                except (mysql.connector.Error, Exception) as ex:
                    log_message(f"Error checking rows in destination database `{db}`: {ex}", LogType.ERROR)
                    close_pbar(progress)
                    raise ex

            # Close the progress bar
            close_pbar(progress)

        # Close database connections
        close_handlers(src_cur, dst_cur, src_conn, dst_conn)

        # Compare row counts between source and destination
        if len(src_sizes) == 0 and len(dst_sizes) == 0:
            log_message("No data was migrated.", LogType.WARNING)
            return

        if src_sizes == dst_sizes and len(src_sizes) > 0 and len(dst_sizes) > 0:
            # Success case
            if not failed_dbs:
                log_message(f"Check OK! Migration of {row_count:,} rows was successfully completed.", LogType.ADD)
            else:
                log_message(f"Check OK of {row_count:,} rows but some databases failed: {', '.join(failed_dbs)}", LogType.WARNING)
        else:
            # Failure case
            log_message("Check KO! Some databases or tables did not migrate correctly.", LogType.ERROR)
            if failed_dbs:
                log_message(f"Failed databases: {', '.join(failed_dbs)}.", LogType.ERROR)

            # Identify databases only in the source or destination
            check_mismatches(src_dbs, dst_dbs, src_sizes, dst_sizes)

    except (mysql.connector.Error, Exception) as ex:
        log_message(f"Error during migration check: {ex}", LogType.ERROR)
        return


def check_mismatches(src_dbs: List[str], dst_dbs: List[str], src_sizes: Dict[str, int], dst_sizes: Dict[str, int]) -> None:
    """
    Logs databases or tables that have different row counts or are only present in the source or destination.

    :param src_dbs: List of source databases.
    :param dst_dbs: List of destination databases.
    :param src_sizes: Row counts in source databases.
    :param dst_sizes: Row counts in destination databases.
    :return: None
    """
    # Detect databases only in the source
    if any(db not in dst_dbs for db in src_dbs):
        log_message("Databases only in source server:", LogType.ERROR)
        for db in src_dbs:
            if db not in dst_dbs:
                log_message(f" - {db}", LogType.ERROR)

    # Detect databases only in the destination
    if any(db not in src_dbs for db in dst_dbs):
        log_message("Databases only in destination server:", LogType.ERROR)
        for db in dst_dbs:
            if db not in src_dbs:
                log_message(f" - {db}", LogType.ERROR)

    # Detect tables with a different number of rows between source and destination
    for table, size in src_sizes.items():
        if table in dst_sizes and dst_sizes[table] != src_sizes[table]:
            log_message(f"Tables with a different number of rows: {table} => {src_sizes[table]} | {dst_sizes[table]}", LogType.ERROR)


def remove_database(db_name: str) -> Tuple[bool, Exception]:
    """
    Removes a single database by calling the remove_databases function.

    :param db_name: The name of the database to remove.
    :return: A tuple indicating the success of the operation and an exception if any occurred.
    """
    return remove_databases([db_name])


def remove_databases(dbs: List[str]) -> Tuple[bool, Exception]:
    """
    Removes a list of databases from the destination MySQL server.

    :param dbs: A list of database names to remove.
    :return: A tuple indicating the success of the operation and an exception if any occurred.
    """
    try:
        # Connect to both source and destination databases
        src_cur, dst_cur, src_conn, dst_conn = connect(set_session_vars=True)

        # Create a progress bar for removing databases
        progress = create_pbar(total=len(dbs), leave=False, colour=PbarColors.DROP, units='database')

        # Loop through each database to remove it
        for db in dbs:
            update_pbar(progress=progress, number=1, message=f"Database [Destination].`{db}` is being removed...", prompt=PbarPrompts.INFO_PROMPT)

            try:
                # Remove the database from the destination server
                dst_cur.execute(f"DROP DATABASE `{db}`")
            except mysql.connector.Error as ex:
                # Only raise the error if it is not the database already exists error (1008)
                if ex.errno != errorcode.ER_DB_DROP_EXISTS:
                    raise ex
    except Exception as ex:
        return False, ex
    finally:
        # Close the progress bar
        close_pbar(progress)

        # Close all database connections and cursors
        close_handlers(src_cur, dst_cur, src_conn, dst_conn)

    return True, None


def handle_failed_databases() -> None:
    """
    Logs the failed databases and provides instructions for retrying their migration.

    :return: None
    """
    failed_databases = get_failed_dbs()
    log_message(f"{Style.RESET_ALL}Some databases failed to migrate. If you execute the same process, this script will only try failed databases. "
                "If you want the complete process to be executed, please remove file failed_databases.log", LogType.WARNING)
    log_message("Migration check will fail", LogType.WARNING)
    log_message(f"Failed databases are: {failed_databases}", LogType.WARNING)


def handle_grants_migration_warning(src_conn: CMySQLConnection, dst_conn: CMySQLConnection) -> None:
    """
    Handles warnings for migrating grants between different MySQL versions.

    :param src_conn: The source MySQL connection.
    :param dst_conn: The destination MySQL connection.
    :return: None
    """
    if src_conn.get_server_info() != dst_conn.get_server_info():
        warning_message = (
            f"  - {Fore.YELLOW}Migrating users between different database versions may result in an "
            f"inaccessible target environment.{Style.RESET_ALL}"
        )
        log_message(warning_message, LogType.WARNING)

        log_message(
            f"  - {Fore.YELLOW}Please cancel the process if you are unsure and restart it without "
            f"the --migrate-grants flag!{Style.RESET_ALL}", LogType.WARNING
        )

        # Progress bar to give the user time to cancel the process
        progress = tqdm(total=10, leave=False, colour="#30a5ab", unit='table')
        for second in range(10):
            progress.update(1)
            progress.set_description(
                f"[{Fore.YELLOW}#{Style.RESET_ALL}]   - {Fore.YELLOW}Waiting 10 seconds in case you want to "
                f"cancel (ctrl+c) the process{Style.RESET_ALL}."
            )
            sleep(1)

        close_pbar(progress)
