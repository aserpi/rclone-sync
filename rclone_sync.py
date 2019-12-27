"""Bi-directional sync for rclone.

For more info: https://github.com/aserpi/rclone-sync
"""
import argparse
import atexit
import hashlib
import pathlib
import re
import subprocess
import sys
import tempfile
from typing import List, Optional, Set, Tuple, Union

__version__ = "0.0.1"

PathLike = Union[pathlib.Path, str]


def delete_lock_file(lock_file: pathlib.Path) -> None:
    """Deletes the lock file."""
    lock_file.unlink()


def get_paths_id(path_1: str, path_2: str) -> str:
    """Returns a determininstic (nearly-)unique id for the paths.

    Directly concatenating the two paths produces a determininstic
    unique identifier but, since its size is not constrained a priori,
    it can not be used as a file name.

    The hexadecimal SHA-256 hash digest of the concatenation of the two
    paths is guaranteed to be collision resistant, therefore it can
    be used as an id.
    """
    paths_hash = hashlib.sha256()
    paths_hash.update(path_1.encode())
    paths_hash.update(path_2.encode())

    return paths_hash.hexdigest()


def list_remotes() -> Set[str]:
    """Lists the remotes configured in rclone."""
    return set(re.findall(r"(\S+):",
                          subprocess.run(["rclone", "listremotes"], capture_output=True,
                                         check=False, text=True).stdout))  # yapf: disable


# TODO (aserpi): add Windows support
def resolve_path(path: PathLike, remotes: Union[List[str], Set[str]]) -> Optional[PathLike]:
    """Resolves the path.

    Returns:
        None if the path can not be used by rclone.
        A pathlib.Path object for an absolute path if the path is local.
        A str if the path is for a remote storage.
    """
    if isinstance(path, str):  # local or remote path
        if not path:
            print("Empty path")
            return None
        path_split = path.split(":", 1)
        if len(path_split) == 1:
            path = pathlib.Path(path)
        else:
            # See https://rclone.org/docs/#copying-files-or-directories-with-in-the-names
            if "/" in path_split[0]:
                path = pathlib.Path(path)
            elif path_split[0] in remotes:  # remote path
                ret = subprocess.run(["rclone", "mkdir", path],
                                     capture_output=True,
                                     check=False,
                                     text=True)
                if ret.returncode:
                    print(ret.stderr)
                    return None
                return path
            else:  # Also handles the case where path is ":"
                print(f"Remote '{path_split[0]}' was not found")
                return None

    if isinstance(path, pathlib.Path):  # local path
        path = path.expanduser().resolve(strict=False)
        try:
            path.mkdir(exist_ok=True, parents=True)
        except OSError as e:
            print(f"Can not use path '{path}': "
                  f"got '{e.strerror}' when creating directory '{e.filename}'")
            return None
        return path

    raise TypeError("Path must be 'str' or 'pathlib.Path'")


def resolve_paths(path_1: PathLike,
                  path_2: PathLike) -> Tuple[Optional[PathLike], Optional[PathLike]]:
    """Resolves the two paths.

    Returns:
        See resolve_path. In addition, it returns (None, None) if the
        paths are identical.
    """
    remotes = list_remotes()
    absolute_path_1 = resolve_path(path_1, remotes)
    absolute_path_2 = resolve_path(path_2, remotes)

    if absolute_path_1 is not None and absolute_path_1 == absolute_path_2:
        print("The two paths are identical!")
        return None, None

    return absolute_path_1, absolute_path_2


def main() -> None:
    """Main function."""
    parser = argparse.ArgumentParser(description="Bi-directional sync for rclone.")

    # Positional arguments
    parser.add_argument("path_1", help="first path")
    parser.add_argument("path_2", help="second path")

    # Optional arguments
    parser.add_argument("-V", "--version", action="version", version=f"%(prog)s v{__version__}")

    args = parser.parse_args()

    path_1, path_2 = resolve_paths(args.path_1, args.path_2)
    if path_1 is None or path_2 is None:
        sys.exit(2)

    # Since path order is important, using always the same (for
    # different runs) greatly simplifies application logic.
    path_1_str = str(path_1)
    path_2_str = str(path_2)
    if path_1_str > path_2_str:
        path_1_str, path_2_str = path_2_str, path_1_str
    paths_id = get_paths_id(path_1_str, path_2_str)

    # Use a lock file to avoid simultaneous identical runs.
    locks_dir = pathlib.Path(tempfile.gettempdir()) / "rclone-sync"
    locks_dir.mkdir(exist_ok=True)
    lock_file = locks_dir / paths_id
    try:
        lock_file.touch(exist_ok=False)
    except FileExistsError:
        print("rclone-sync is already synchronising the two paths.\n"
              "If it is not the case, delete the file '%s'.", lock_file)  # yapf: disable
        sys.exit(1)
    atexit.register(delete_lock_file, lock_file)

    raise NotImplementedError


if __name__ == "__main__":
    main()
