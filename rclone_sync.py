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
from typing import Any, Dict, List, Optional, Set, Tuple, Union  # TODO: PEP 585

__version__ = "0.0.1"

PathLike = Union[pathlib.Path, str]



def check_rclone_config(rclone_path: PathLike = "rclone",
                        rclone_config: Optional[PathLike] = None) -> None:
    """Checks for the existence of rclone's config file."""
    args = [rclone_path, "config", "file"]
    if rclone_config is not None:
        args.append("--config")
        args.append(rclone_config)

    try:
        config_file_response = subprocess.run(args, capture_output=True, check=False, text=True)
    except FileNotFoundError:
        print(f"Cannot find rclone executable file '{rclone_path}'")
        sys.exit(3)

    if config_file_response.returncode:
        print("Cannot find rclone configuration file")
        sys.exit(4)

    config_file = pathlib.Path(config_file_response.stdout.split("\n", 1)[1].strip())
    try:
        if not config_file.is_file():
            print(f"rclone configuration file '{config_file}' does not exist")
            sys.exit(4)
    except OSError as e:
        print(f"Cannot use rclone configuration file '{rclone_config}': "
              f"got '{e.strerror}' on '{e.filename}'")
        sys.exit(4)


def delete_lock_file(lock_file: pathlib.Path) -> None:
    """Deletes the lock file."""
    lock_file.unlink()


def get_paths_id(path_1: str, path_2: str) -> str:
    """Returns a deterministic (nearly-)unique id for the paths.

    Directly concatenating the two paths produces a determininstic
    unique identifier but, since its size is not constrained a priori,
    it cannot be used as a file name.

    The hexadecimal SHA-256 hash digest of the concatenation of the two
    paths is guaranteed to be collision resistant, therefore it can
    be used as an id.
    """
    paths_hash = hashlib.sha256()
    paths_hash.update(path_1.encode())
    paths_hash.update(path_2.encode())

    return paths_hash.hexdigest()


def list_remotes(rclone_path: PathLike = "rclone",
                 rclone_config: Optional[PathLike] = None) -> Set[str]:
    """Lists the remotes configured in rclone."""
    args = [rclone_path, "listremotes"]
    if rclone_config is not None:
        args.append("--config")
        args.append(rclone_config)

    return set(
        re.findall(r"(\S+):",
                   subprocess.run(args, capture_output=True, check=False, text=True).stdout))

# TODO (aserpi): add Windows support
def resolve_path(path: PathLike,
                 remotes: Union[List[str], Set[str]],
                 rclone_path: PathLike = "rclone",
                 rclone_config: Optional[PathLike] = None) -> Optional[PathLike]:
    """Resolves the path.

    Returns:
        None if the path cannot be used by rclone.
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
                args = [rclone_path, "mkdir", path]
                if rclone_config:
                    args.append("--config")
                    args.append(rclone_config)
                ret = subprocess.run(args, capture_output=True, check=False, text=True)
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
            print(f"Cannot use path '{path}': "
                  f"got '{e.strerror}' when creating directory '{e.filename}'")
            return None
        return path

    raise TypeError("Path must be 'str' or 'pathlib.Path'")


def resolve_paths(
        path_1: PathLike,
        path_2: PathLike,
        rclone_path: PathLike = "rclone",
        rclone_config: Optional[PathLike] = None) -> Tuple[Optional[PathLike], Optional[PathLike]]:
    """Resolves the two paths.

    Returns:
        See resolve_path. In addition, it returns (None, None) if the
        paths are identical.
    """
    remotes = list_remotes(rclone_path=rclone_path, rclone_config=rclone_config)
    absolute_path_1 = resolve_path(path_1,
                                   remotes,
                                   rclone_path=rclone_path,
                                   rclone_config=rclone_config)
    absolute_path_2 = resolve_path(path_2,
                                   remotes,
                                   rclone_path=rclone_path,
                                   rclone_config=rclone_config)

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
    parser.add_argument("-r", "--rclone", help="rclone executable file", type=pathlib.Path)
    parser.add_argument("--retries", help="number of retries", type=int)
    parser.add_argument("--rclone-config", help="rclone configuration file", type=pathlib.Path)
    parser.add_argument("-V", "--version", action="version", version=f"%(prog)s v{__version__}")
    parser.add_argument("-w", "--working-directory", help="directory in which store the db files")

    args = parser.parse_args()

    other_args: Dict[str, Any] = {}
    if args.rclone:
        other_args["rclone_path"] = args.rclone
    if args.rclone_config:
        other_args["rclone_config"] = args.rclone_config
    check_rclone_config(**other_args)

    path_1, path_2 = resolve_paths(args.path_1,
                                   args.path_2,
                                   rclone_path=rclone,
                                   rclone_config=rclone_config)
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
        print(f"rclone-sync is already synchronising the two paths.\n"
              f"If it is not the case, delete the file '{lock_file}'.")
        sys.exit(1)
    atexit.register(delete_lock_file, lock_file)

    if args.retries:
        other_args["retries"] = args.retries
    if args.working_directory:
        other_args["working_dir"] = args.working_directory

    raise NotImplementedError


if __name__ == "__main__":
    main()
