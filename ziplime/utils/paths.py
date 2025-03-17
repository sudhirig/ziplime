"""
Canonical path locations for zipline data.

Paths are rooted at $ZIPLINE_ROOT if that environment variable is set.
Otherwise default to expanduser(~/.zipline)
"""

import os
from pathlib import Path
from typing import Iterable, List

import pandas as pd


def hidden(path: str) -> bool:
    """Check if a path is hidden.

    Parameters
    ----------
    path : str
        A filepath.
    """
    # return os.path.split(path)[1].startswith(".")
    return Path(path).stem.startswith(".")


def ensure_directory(path: str) -> None:
    """Ensure that a directory named "path" exists."""
    Path(path).mkdir(parents=True, exist_ok=True)


def ensure_directory_containing(path: str) -> None:
    """Ensure that the directory containing `path` exists.

    This is just a convenience wrapper for doing::

        ensure_directory(os.path.dirname(path))
    """
    ensure_directory(str(Path(path).parent))


def ensure_file(path: str) -> None:
    """Ensure that a file exists. This will create any parent directories needed
    and create an empty file if it does not exist.

    Parameters
    ----------
    path : str
        The file path to ensure exists.
    """
    if os.path.isfile(path):
        return
    ensure_directory_containing(path)
    Path(path).touch(exist_ok=False)


def last_modified_time(path: str) -> pd.Timestamp:
    """Get the last modified time of path as a Timestamp."""
    return pd.Timestamp(Path(path).stat().st_mtime, unit="s", tz="UTC")


def modified_since(path: str, dt: pd.Timestamp) -> bool:
    """Check whether `path` was modified since `dt`.

    Returns False if path doesn't exist.

    Parameters
    ----------
    path : str
        Path to the file to be checked.
    dt : pd.Timestamp
        The date against which to compare last_modified_time(path).

    Returns
    -------
    was_modified : bool
        Will be ``False`` if path doesn't exist, or if its last modified date
        is earlier than or equal to `dt`
    """
    return Path(path).exists() and last_modified_time(path) > dt


def zipline_root() -> str:
    """Get the root directory for all zipline-managed files.

    For testing purposes, this accepts a dictionary to interpret as the os
    environment.

    Parameters
    ----------

    Returns
    -------
    root : string
        Path to the zipline root dir.
    """

    root = os.environ.get("ZIPLINE_ROOT", None)
    if root is None:
        root = str(Path.expanduser(Path("~/.zipline")))

    return root


def zipline_path(paths: List[str]) -> str:
    """Get a path relative to the zipline root.

    Parameters
    ----------
    paths : list[str]
        List of requested path pieces.
    Returns
    -------
    newpath : str
        The requested path joined with the zipline root.
    """
    return str(Path(zipline_root() / Path(*paths)))


def data_root() -> str:
    """The root directory for zipline data files.

    Parameters
    ----------

    Returns
    -------
    data_root : str
       The zipline data root.
    """
    return zipline_path(["data"])


def data_path(paths: Iterable[str]) -> str:
    """Get a path relative to the zipline data directory.

    Parameters
    ----------
    paths : iterable[str]
        List of requested path pieces.

    Returns
    -------
    newpath : str
        The requested path joined with the zipline data root.
    """
    return zipline_path(["data"] + list(paths))


def cache_root() -> str:
    """The root directory for zipline cache files.

    Parameters
    ----------

    Returns
    -------
    cache_root : str
       The zipline cache root.
    """
    return zipline_path(["cache"])


def ensure_cache_root() -> None:
    """Ensure that the data root exists."""
    ensure_directory(cache_root())


def cache_path(paths: Iterable[str], ) -> str:
    """Get a path relative to the zipline cache directory.

    Parameters
    ----------
    paths : iterable[str]
        List of requested path pieces.

    Returns
    -------
    newpath : str
        The requested path joined with the zipline cache root.
    """
    return zipline_path(["cache"] + list(paths))
