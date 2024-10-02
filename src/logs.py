from enum import Enum
from colorama import Fore, Style


class LogType(Enum):
    """
    This simple Enum represents a log type and its associated colors and symbols.

    Attributes:
        INFO: Represents an informational message.
        QUESTION: Represents a question or query message.
        WARNING: Represents a warning message.
        ERROR: Represents an error message.
        CRITICAL: Represents a critical error message.
        ADD: Represents a message for additions.
        COMMENT: Represents a comment message.
        CODE: Represents a code-related message.
    """
    INFO = (Fore.BLUE, "!")
    QUESTION = (Fore.MAGENTA, "?")
    WARNING = (Fore.YELLOW, "#")
    ERROR = (Fore.RED, "!")
    CRITICAL = (Fore.RED, "#")
    ADD = (Fore.GREEN, "+")
    COMMENT = (Fore.BLUE, "i")
    CODE = (Fore.CYAN, "@")


def get_log_message(message: str, log_type: LogType = LogType.INFO, will_continue: bool = False, is_continuation: bool = False) -> str:
    """
    Generate a formatted log message with a symbol and color.

    :param message: The actual message to log.
    :param log_type: The type of log message (INFO, ERROR, etc.).
    :param will_continue: If True, the message will indicate continuation.
    :param is_continuation: If True, no prefix will be added.
    :return: The formatted log message string.
    """
    color, symbol = log_type.value
    prefix = f"[{color}{symbol}{Style.RESET_ALL}] " if not is_continuation else ""
    indent = "  " if is_continuation else ""

    # Return the formatted log message
    return f"{Style.RESET_ALL}{prefix}{color}{indent}{Style.RESET_ALL}{message}"


def log_message(message: str, log_type: LogType = LogType.INFO, will_continue: bool = False, is_continuation: bool = False) -> None:
    """
    Print a log message to the console, with formatting based on log type.

    :param message: The message to log.
    :param log_type: The type of log (INFO, ERROR, etc.). Defaults to INFO.
    :param will_continue: If True, no new line will be added at the end.
    :param is_continuation: If True, the log is a continuation, and no prefix is added.
    :return: None
    """
    end = '' if will_continue else '\n'

    # Print the formatted log message
    print(f"{get_log_message(message=message, log_type=log_type, will_continue=will_continue, is_continuation=is_continuation)}", end=end)


def log_raw_message(message: str, log_type: LogType = LogType.INFO) -> None:
    """
    Print a raw message to the console with the log type's color, but no additional formatting.

    :param message: The raw message to log.
    :param log_type: The type of log (INFO, ERROR, etc.). Defaults to INFO.
    :return: None
    """
    color, _ = log_type.value

    # Print the raw message without additional formatting
    print(f"{Style.RESET_ALL}{color}{message}{Style.RESET_ALL}")
