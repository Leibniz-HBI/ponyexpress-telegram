"""Test suite for package-level items."""
import re

from ponyexpress_telegram import __version__


def test_version():
    """Should be the current version number."""
    assert __version__ == "1.0.0"


def test_version_type():
    """Should be a string."""
    assert isinstance(__version__, str)


def test_version_number_is_semver():
    """Should be a semver."""
    assert re.fullmatch(r"\d+\.\d+\.\d+(-\w+)?", __version__) is not None
