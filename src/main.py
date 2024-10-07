import argparse
import concurrent.futures
import os
import time as tm
import signal
import math
from tqdm.std import tqdm
from time import sleep
from logs import log_message, Fore, Style, LogType
from typing import List, Dict
from progress import create_pbar, update_pbar, close_pbar, update_pos_pbar, PbarColors, PbarPrompts, get_color_for_progress
from db import close_handlers, get_process_dbs, connect, get_all_tables, count_migration_rows, migrate_grants, check_process, remove_databases, handle_grants_migration_warning, migrate_database
from failed import get_failed_dbs, remove_failed_databases
from config import source_config, destination_config
from datetime import datetime


def wait_progress(seconds: int = 10, message: str = None, add_msg: bool = False) -> None:
    """
    Displays a progress bar for a specified number of seconds, allowing the user to cancel the process.

    :param seconds: The total time in seconds to wait.
    :param message: The message to display during the wait.
    :param add_msg: If True, a closing message is added when the progress bar finishes.
    :return: None
    """
    if message is None:
        message = f"{Fore.YELLOW}Waiting {seconds} seconds in case you want to " \
                  f"cancel (ctrl+c) the process{Style.RESET_ALL}."

    # Create progress bar for waiting period
    progress = create_pbar(total=seconds, leave=False, colour=PbarColors.WAIT, units='second')

    # Wait for the specified number of seconds
    for second in range(seconds):
        # Update progress bar and display the waiting message
        update_pbar(progress=progress, number=1, message=message, prompt=PbarPrompts.WAIT_PROMPT)
        sleep(1)

    # Close the progress bar
    close_pbar(progress, add_msg)


def migration_process(args: Dict = None) -> List[str]:
    """
    Runs the migration process for databases, handling connections, multithreading, and progress reporting.

    :param args: Arguments dict containing options for the migration process.
    :return: List of strings with processed databases
    """
    count_records_before_start = False

    # Connect to the source and destination databases
    try:
        src_cur, dst_cur, src_conn, dst_conn = connect(set_session_vars=False)
    except Exception as ex:
        log_message(f"Could not connect to databases: {ex}", LogType.CRITICAL)
        exit(1)

    # Load source, destination, and failed databases
    src_dbs = get_process_dbs(src_cur)
    dst_dbs = get_process_dbs(dst_cur)
    failed_dbs = get_failed_dbs()
    skip_dbs = 0

    # Clear the failed databases log
    remove_failed_databases()

    # If there are failed databases, only process those
    if failed_dbs:
        src_dbs = [db for db in src_dbs if db in failed_dbs]
        dst_dbs = [db for db in dst_dbs if db in failed_dbs]

    # If the user opts to skip existing databases on the destination
    if args.skip_dbs:
        src_dbs = [db for db in src_dbs if db not in dst_dbs]
        skip_dbs = len(dst_dbs) - len(src_dbs)
        dst_dbs = src_dbs

    # Get all tables from the source databases
    tables = get_all_tables(src_dbs, src_cur)

    # Close all database handlers
    close_handlers(src_cur, dst_cur, src_conn, dst_conn)

    # Logging information
    databases_to_migrate = max(0, len(src_dbs) - skip_dbs)
    log_message("Process will run using:", LogType.INFO)
    log_message(f"  - {databases_to_migrate:,} databases to migrate. {skip_dbs:,} to skip.", LogType.COMMENT)
    log_message(f"  - {len(tables):,} tables to migrate" if databases_to_migrate > 0 else "  - 0 tables to migrate", LogType.COMMENT)

    # If counting records before starting
    if count_records_before_start:
        rows_to_migrate = count_migration_rows(src_dbs)
        log_message(f"  - {rows_to_migrate:,} rows to migrate", LogType.COMMENT)

    # Thread and batch size information
    log_message(f"  - {args.db_thcount} thread workers for databases and {args.table_thcount} for tables. {(args.db_thcount * args.table_thcount)} can run simultaneously. This machine has {os.cpu_count()} cores.", LogType.COMMENT)
    log_message(f"  - Inserts will be applied in groups of {args.batch_size:,}", LogType.COMMENT)

    # Database skipping and dropping options
    log_message(f"  - Existing databases will be skipped: {args.skip_dbs}", LogType.COMMENT)
    log_message(f"  - Existing databases will be dropped: {not args.skip_dbs and not args.keep_dbs}", LogType.COMMENT)

    # Handle grants migration if selected
    if args.grants:
        log_message("  - All grants will be migrated!", LogType.COMMENT)
        handle_grants_migration_warning(src_conn, dst_conn)

    # MySQL version and migration direction
    log_message(
        f"  - @MySQL Migration direction: {source_config['host']}:{source_config['port']} "
        f"-> {destination_config['host']}:{destination_config['port']}", LogType.COMMENT
    )
    log_message(
        f"  - @MySQL Version: {src_conn.get_server_info()} -> {dst_conn.get_server_info()}", LogType.COMMENT
    )

    log_message("Inspection done!", LogType.INFO)
    sleep(1)

    # Start migration if there are databases to process
    if len(src_dbs) > 0:
        # Remove existing databases from the destination if we have any in dst_dbs
        if len(dst_dbs) > 0:
            # Wait to permit user to cancel this process
            log_message("Launching target drop databases", LogType.INFO)
            wait_progress(seconds=3)

            # Remove databases
            removed, ex = remove_databases(dst_dbs)
            if not removed:
                log_message(f"Drop databases failed! Error was: {ex}", LogType.ERROR)
                return

            log_message("Target databases removed", LogType.ADD)

        # Verbose and wait again
        log_message("Launching migration threads", LogType.INFO)
        wait_progress(seconds=3)

        # Create progress bar for the overall process
        progress = create_pbar(len(src_dbs) + 2, leave=True, colour=PbarColors.DATABASE, units='database')
        update_pos_pbar(progress, 0)
        update_pbar(progress=progress, number=1, message="Overall process", prompt=PbarPrompts.PERCENT_PROMPT)

        # Execute the migration process using multithreading
        all_process_ok = run_migration_threads(src_dbs, args, progress)

        # Update progress status
        progress.colour = 'green' if all_process_ok else 'red'
        update_pbar(progress=progress, number=1, message="Overall process finished!", prompt=(PbarPrompts.ADD_PROMPT if all_process_ok else PbarPrompts.ERROR_PROMPT))

        # Migrate grants?
        if args.grants:
            migrate_grants()
    else:
        log_message("No databases were scheduled to be migrated.", LogType.ADD)

    # Return processed databases
    return src_dbs


def run_migration_threads(src_dbs: List[str], args: Dict, progress: tqdm) -> bool:
    """
    Runs the migration threads for each database using a ThreadPoolExecutor.

    :param src_dbs: The list of source databases to migrate.
    :param args: The migration arguments.
    :param progress: The progress bar to update during migration.
    :return: True if all processes succeeded, False otherwise.
    """
    all_process_ok = True
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.db_thcount, thread_name_prefix='mysql-migrator') as executor:
        futures = []

        # Submit each database migration as a separate thread
        for db_name in src_dbs:
            futures.append(executor.submit(lambda db=db_name: (db, migrate_database(db, args))))

        # Wait for all threads to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception:
                all_process_ok = False

            update_pbar(progress=progress, colour=get_color_for_progress(progress.n / progress.total), number=1, message="Overall process", prompt=PbarPrompts.PERCENT_PROMPT)
            sleep(0.05)

    return all_process_ok


def seconds_to_time(seconds: float = 0.0) -> str:
    """
    Converts a float value representing seconds into a formatted string (hh:mm:ss).

    :param seconds: Time in seconds.
    :return: Formatted string in the format "hh:mm:ss".
    """
    # Calculate hours, minutes, and seconds
    hours, rest = divmod(seconds, 3600)
    minutes, rest_secs = divmod(rest, 60)

    # Return formatted time
    return f"{int(hours):02}h:{int(minutes):02}min:{rest_secs:04.1f}s"


def signal_handler(sig, frame) -> None:
    """
    Handles the SIGINT signal (Ctrl+C) to safely exit the program.

    :param sig: The signal received.
    :param frame: The current stack frame.
    :return: None
    """
    sleep(1)
    print(f"{Style.RESET_ALL}", flush=True)
    log_message(f"{Fore.RED}The process was cancelled by the user{Style.RESET_ALL}!", LogType.ERROR)
    exit(0)


def parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments for the migration script.

    :return: Parsed arguments as a namespace.
    """
    parser = argparse.ArgumentParser(description='mysql.migrator tool. Use it to migrate between MySQL database instances.')

    # Get default value for db_thcount and table_thcount params
    db_thcount = int(math.sqrt(os.cpu_count()))
    table_thcount = int(math.sqrt(os.cpu_count()))

    # Add arguments
    parser.add_argument('-b', '--batch-size', type=int, default=2048, dest='batch_size',
                        help='Optional. Sets the number of records to migrate in each batch. Default is 2048.')

    parser.add_argument('-s', '--skip-existing-dbs', action='store_true', dest='skip_dbs',
                        help='Optional. Skip migration of databases that already exist on the destination. By default, all databases are migrated.')

    parser.add_argument('-d', '--keep-existing-dbs', action='store_true', dest='keep_dbs',
                        help='Optional. Keep existing databases on the destination before migration. By default, destination databases are dropped.')

    parser.add_argument('-g', '--migrate-grants', action='store_true', dest='grants',
                        help='Optional. Migrate grants between MySQL instances. By default, grants are not migrated unless this flag is set.')

    parser.add_argument('-t', '--thread-db', type=int, default=db_thcount, dest='db_thcount',
                        help=f'Optional. Number of threads to use for database migration. Default value will be {db_thcount}.')

    parser.add_argument('-x', '--thread-table', type=int, default=table_thcount, dest='table_thcount',
                        help=f'Optional. Number of threads to use (for every database) for table migration. Default value will be {table_thcount}.')

    parser.add_argument('-c', '--check-only', action='store_true', dest='check',
                        help='Optional. Only check the last migration process. No changes will be made.')

    # Parse arguments
    return parser.parse_args()


if __name__ == "__main__":
    # Pick start time
    start_time = tm.time()

    # Initial verbose log
    log_message(f"Starting at {Fore.BLUE}{datetime.now().strftime('%I:%M%p on %B %d, %Y')}{Style.RESET_ALL}...", LogType.INFO)

    # Collect arguments from command line
    args = parse_args()

    # Set stop signal function (Ctrl+C handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Run the migration process if not in "check-only" mode
    processed_dbs = None
    if not args.check:
        processed_dbs = migration_process(args)

    # Log that the migration process has finished
    log_message("Data migration process finished", LogType.INFO)

    # Always run the check process after the migration
    check_process(args.skip_dbs, args.check, processed_dbs)

    # Log the total time taken for migration
    log_message(f"{'Migration' if args.check else 'Check'} finished in {Fore.BLUE}{seconds_to_time(round(tm.time() - start_time, 1))}{Style.RESET_ALL} "
                f"at {Fore.BLUE}{datetime.now().strftime('%I:%M%p on %B %d, %Y')}{Style.RESET_ALL}", LogType.INFO)
    log_message("Bye!", LogType.INFO)
    sleep(2)

    # Exit the program
    exit(0)
