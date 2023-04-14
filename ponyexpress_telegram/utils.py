"""This module contains utility functions and classes.

Author: Philipp Kessling
Date: 2023-04-19
Institution: Leibniz-Institute for Media Research | Hans-Bredow-Institut (HBI)
"""
import time
from datetime import datetime
from functools import partial, reduce, wraps
from typing import Any, Callable, Dict, Tuple

from loguru import logger as log


class WaitTimed:
    """Decorator that waits a given amount of seconds between function calls.

    Args:
        seconds (int): the amount of seconds to wait between function calls

    Example:
        >>> @WaitTimed(5)
        ... def foo():
        ...     print("foo")
        >>> foo()
        foo
        >>> foo() # will wait 5 seconds
        foo
    """

    def __init__(self, seconds):
        self.seconds = seconds
        self.last_time = None

    def __call__(self, func):
        @wraps(func)
        def timer_wrapper(*args, **kwargs):
            now = datetime.now()
            if (
                self.last_time is None
                or (now - self.last_time).total_seconds() > self.seconds
            ):
                self.last_time = now

                log.info(f"Running {func.__name__} at {self.last_time}")
            else:
                sleep = self.seconds - (now - self.last_time).total_seconds()
                log.info(f"Sleeping for {sleep} seconds")
                time.sleep(sleep)
                self.last_time = now
                log.info(f"Running {func.__name__} at {self.last_time} (after sleep)")

            return func(*args, **kwargs)

        return timer_wrapper

    def set_wait_time(self, seconds: int):
        """Sets the wait time in seconds."""
        self.seconds = seconds


def extract_from(tree, xpaths):
    """extracts data from a lxml tree based on a dictionary of xpaths"""
    return {name: tree.xpath(xpath) for name, xpath in xpaths.items()}


def cleanup(
    data_dict: Dict[str, Any], rules: Dict[Callable, Callable]
) -> Dict[str, Any]:
    """Rule-based data cleaning.

    Args:
        data_dict: the data to be cleaned
        rules: the rules to be applied, whereas a ruleset is a dictionary with predicates as keys
            and cleaning functions as values. The predicate is a function that takes the key and
            value of the data_dict and returns a boolean. If the predicate returns True, the
            corresponding cleaning function is applied to the value.
    Returns:
        Dict[str, Any] : the cleaned data

    Example:
        >>> data = {"name": "@foo", "age": "42", "email": ""}
        >>> rules = {
        ...     lambda key, value: key == "name": lambda x: x.replace("@", ""),
        ...     lambda key, value: isinstance(value, str) and value.isnumeric(): int,
        ...     lambda key, value: value == "": lambda x: None,
        ... }
        >>> cleanup(data, rules)
        {'name': 'foo', 'age': 42, email: None}
    """

    def _cleanup_reducer(
        value: Any,
        ruleset: Tuple[Callable[[str, Any], bool], Callable[[Any], Any]],
        key: str,
    ) -> Any:
        predicate, cleaning_function = ruleset
        if predicate(key, value) is True:
            return cleaning_function(value)
        return value

    return {
        key: reduce(partial(_cleanup_reducer, key=key), list(rules.items()), value)
        for key, value in data_dict.items()
    }
