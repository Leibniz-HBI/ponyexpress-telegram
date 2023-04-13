"""Welcome to ponyexpress_telegram."""
import sys
from pathlib import Path
from typing import List

import click
from loguru import logger as log

from . import __version__, telegram_connector

log_levels = {0: "ERROR", 1: "WARNING", 2: "INFO", 3: "DEBUG", 4: "TRACE"}


@click.command()
@click.version_option(__version__)
@click.argument("names", type=str, nargs=-1)
@click.option("--messages-output", "-m", type=click.File("a"), default="-", help="")
@click.option("--users-output", "-u", type=click.File("a"), default="-")
@click.option("--prepare-edges", "-p", is_flag=True)
@click.option("--log-file", "-l", type=click.Path())
@click.option("-v", "--verbose", count=True, default=0)
def cli(
    names: List[str], messages_output, users_output, prepare_edges: bool, log_file: Path,
    verbose: int
) -> None:
    """Scrape a Telegram Channel"""
    log.remove()  # remove all default loggers
    # and set the logging level according to user input
    _log_level_ = log_levels.get(verbose) or "CRITICAL"
    _sink_ = log_file or sys.stdout  # if a log_file is specified omit logging to stdout
    log.add(_sink_, level=_log_level_)

    edges, users = telegram_connector(names, {"prepare_edges": prepare_edges})

    edges.to_json(messages_output, orient="records", lines=True)
    users.to_json(users_output, orient="records", lines=True)
