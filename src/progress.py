from tqdm import tqdm, std
from enum import Enum
from colorama import Fore, Style
from logs import LogType, get_log_message
from typing import Tuple


class PbarPrompts(Enum):
    """Enum for different progress bar prompts with colors."""
    NONE = ""
    PERCENT_PROMPT = f"[{Fore.CYAN}%{Style.RESET_ALL}] "
    INFO_PROMPT = f"[{Fore.BLUE}!{Style.RESET_ALL}] "
    WAIT_PROMPT = f"[{Fore.YELLOW}#{Style.RESET_ALL}] "
    ERROR_PROMPT = f"[{Fore.RED}!{Style.RESET_ALL}] "
    ADD_PROMPT = f"[{Fore.GREEN}+{Style.RESET_ALL}] "


class PbarColors(Enum):
    """Enum for different colors used in the progress bars."""
    DROP = "#d91a43"
    DATABASE = "#3E6FCE"
    TABLE = "#cc995e"
    THROTTLED = "#cc745e"
    DATA = "#cc995e"
    INFO = "#cc33ba"
    WAIT = "#30a5ab"


def create_pbar(total: int, colour: PbarColors, units: str, leave: bool = False) -> std.tqdm:
    """
    Create a progress bar with the specified total, color, and unit type.

    :param total: The total number of iterations.
    :param colour: Color of the progress bar from PbarColors enum.
    :param units: Unit of measurement for the progress bar.
    :param leave: Whether or not to leave the progress bar displayed after completion.
    :return: A tqdm progress bar instance.
    """
    # Create and return the progress bar with specified parameters
    progress = tqdm(total=total, leave=leave, colour=colour.value, unit=units)
    return progress


def update_pos_pbar(progress: std.tqdm, position: int = 0) -> None:
    """
    Update the position of the progress bar.

    :param progress: The progress bar to update.
    :param position: The new position for the progress bar. Defaults to 0.
    :return: None
    """
    # Update the position of the progress bar
    progress.pos = position


def update_pbar(progress: std.tqdm, number: int, message: str, prompt: PbarPrompts = PbarPrompts.NONE, colour: str = None) -> None:
    """
    Update the progress bar with a new value, message, and optional prompt and color.

    :param progress: The progress bar to update.
    :param number: The number to update the progress bar by.
    :param message: A message to display in the progress bar.
    :param prompt: A prompt to prepend to the message from PbarPrompts enum.
    :param colour: An optional color to change the progress bar's color.
    :return: None
    """
    # Set the progress bar color if provided
    if colour:
        progress.colour = colour

    # Set the description with prompt and message
    progress.set_description(f"{prompt.value}{message}")

    # Update the progress bar by the given number
    if number > 0:
        progress.update(number)


def close_pbar(progress: std.tqdm, add_msg: bool = False) -> None:
    """
    Close the progress bar and optionally log a closing message.

    :param progress: The progress bar to close.
    :param add_msg: Whether to log a closing message. Defaults to False.
    :return: None
    """
    # Clear and close the progress bar
    progress.clear()
    progress.close()

    # Optionally log a closing message
    if add_msg:
        progress.write(get_log_message("Closing bars...", LogType.INFO))


def interpolate_color(start_color: Tuple[int, int, int], end_color: Tuple[int, int, int], factor: float) -> str:
    """
    Interpolates between two RGB colors based on a given factor. The function calculates the intermediate color
    between a start and end color, depending on the interpolation factor (ranging from 0 to 1).

    :param start_color: The starting RGB color represented as a tuple of (R, G, B) values.
    :param end_color: The ending RGB color represented as a tuple of (R, G, B) values.
    :param factor: A float between 0 and 1 that determines the ratio of interpolation.
                   A value of 0 returns the start color, and a value of 1 returns the end color.

    :return: The interpolated color as a hex string in the format '#rrggbb'.
    """
    # Interpolate each component (R, G, B) between the start and end color
    interpolated_color = tuple(
        int(start + (end - start) * factor)
        for start, end in zip(start_color, end_color)
    )
    # Convert the interpolated RGB values to a hexadecimal color string
    return '#{:02x}{:02x}{:02x}'.format(*interpolated_color)


def get_color_for_progress(progress: float) -> str:
    """
    Returns a color corresponding to the progress percentage, by interpolating between
    a starting color and an ending color based on progress.

    :param progress: A float representing the progress percentage, ranging from 0 (0%) to 1 (100%).
    :return: A hex color string that represents the interpolated color based on progress.
    """
    # Define the starting and ending RGB colors
    start_color = (42, 99, 209)  # RGB for starting color (blueish)
    end_color = (13, 188, 121)   # RGB for ending color (greenish)

    # Use interpolation to calculate the color based on progress
    # Progress is scaled by 0.45 to adjust the range of the color transition
    return interpolate_color(start_color, end_color, progress * 0.45)
