"""Bi-directional sync for rclone.

For more info: https://github.com/aserpi/rclone-sync
"""
import argparse
import atexit
import dataclasses
import datetime
import hashlib
import pathlib
import re
import subprocess
import sys
import tempfile
from typing import Any, Dict, List, Optional, Set, Tuple, Union  # TODO: PEP 585

__version__ = "0.0.1"

PathLike = Union[pathlib.Path, str]
RCLONE_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclasses.dataclass
class FileAttributes:
    size: Optional[int] = None
    timestamp = datetime.datetime.min


class SyncFile:
    _types = {"db_1", "db_2", "path_1", "path_2"}

    def __init__(self, path):
        self.path = path
        self.db_1 = FileAttributes()
        self.db_2 = FileAttributes()
        self.path_1 = FileAttributes()
        self.path_2 = FileAttributes()

    def add_properties(self, type_: str, size: str, timestamp: str) -> None:
        assert type_ in SyncFile._types
        file_attrs = self.__getattribute__(type_)
        file_attrs.size = int(size)
        file_attrs.timestamp = datetime.datetime.strptime(timestamp, RCLONE_TIMESTAMP_FORMAT)

    def stringify_properties(self, type_: str) -> str:
        timestamp = self.__getattribute__(f"{type_}_timestamp")
        size = self.__getattribute__(f"{type_}_size")
        return f"{self.path};{timestamp};{size}"


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
        sys.exit(10)

    if config_file_response.returncode:
        print("Cannot find rclone configuration file")
        sys.exit(11)

    config_file = pathlib.Path(config_file_response.stdout.split("\n", 1)[1].strip())
    try:
        if not config_file.is_file():
            print(f"rclone configuration file '{config_file}' does not exist")
            sys.exit(11)
    except OSError as e:
        print(f"Cannot use rclone configuration file '{rclone_config}': "
              f"got '{e.strerror}' on '{e.filename}'")
        sys.exit(11)


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


def list_files(path_1: PathLike,  # pylint: disable=too-many-arguments
               path_2: PathLike,
               paths_id: str,
               working_dir: pathlib.Path = pathlib.Path("~/.rclone-sync"),
               retries: int = 1,
               rclone_path: PathLike = "rclone",
               rclone_config: Optional[PathLike] = None) -> Dict[str, SyncFile]:  # yapf: disable
    """TODO: documentation"""
    working_dir = working_dir.expanduser().resolve(strict=False)
    try:
        working_dir.mkdir(exist_ok=True, parents=True)
    except OSError as e:
        print(f"Cannot use the working directory: got {e.strerror} on {e.filename}")
        sys.exit(24)

    files: Dict[str, SyncFile] = {}
    args = [str(rclone_path), "lsf", "-R", "--files-only", "--format", "pts"]
    if rclone_config:
        args.append("--config")
        args.append(str(rclone_config))
    args.append(str(path_1))

    list_files_in_path(args, 1, files, retries)
    args.pop()
    args.append(str(path_2))
    list_files_in_path(args, 2, files, retries)
    args.pop()

    raise NotImplementedError



def list_files_in_path(args: List[str],
                       path_number: int,
                       files: Dict[str, SyncFile],
                       retries: int = 1) -> bool:
    """Retrieves file properties from a path.

    Args:
        args: command-line arguments
        path_number: path number
        files: dictionary in which insert the parsed files
        retries: max number of retries
    """
    for i in range(retries):
        ret = subprocess.run(args, capture_output=True, check=False, text=True)

        if not ret.returncode:
            parse_lsf(ret.stdout, f"path_{path_number}", files)
            return True

        print(f"Failed to list files in path_{path_number} ({i + 1}/{retries}): {ret.stderr}")
    sys.exit(3 + path_number)


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


def parse_lsf(lsf_output: str, type_: str, files: Dict[str, SyncFile]) -> None:
    """Parses the output of `rclone lsf`.

    Args:
        lsf_output: the output of a `rclone lsf --files-only --format pts` run.
        type_: the type of path ('path_1', 'path_2', 'lsf_1' or 'lsf_2').
        files: dictionary in which insert the parsed files.
    """
    for f in lsf_output.rstrip().split("\n"):
        f_split = f.rsplit(";", 2)  # Filename may contain semicolons

        try:
            f_obj = files[f_split[0]]  # File already encountered somewhere
        except KeyError:  # First time we see the file
            f_obj = SyncFile(f_split[0])
            files[f_split[0]] = f_obj

        f_obj.add_properties(type_, f_split[2], f_split[1])


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
            path = pathlib.Path(path)  # local path, process in other isinstance branch
        else:
            # If there is a slash in the first part, then the path is local
            # See https://rclone.org/docs/#copying-files-or-directories-with-in-the-names
            if "/" in path_split[0]:
                path = pathlib.Path(path)  # local path, process in other isinstance branch
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
    if not (absolute_path_1 := resolve_path(
            path_1, remotes, rclone_path=rclone_path, rclone_config=rclone_config)):
        sys.exit(1)

    if not (absolute_path_2 := resolve_path(
            path_2, remotes, rclone_path=rclone_path, rclone_config=rclone_config)):
        sys.exit(2)

    if absolute_path_1 == absolute_path_2:
        print("The two paths are identical!")
        sys.exit(3)

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

    path_1, path_2 = resolve_paths(args.path_1, args.path_2, **other_args)

    # Since path order is important, using always the same (for
    # different runs) greatly simplifies application logic.
    path_1_str = str(path_1)
    path_2_str = str(path_2)
    if path_1_str > path_2_str:
        path_1, path_2 = path_2, path_1
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
        sys.exit(23)
    atexit.register(delete_lock_file, lock_file)

    if args.retries:
        other_args["retries"] = args.retries
    if args.working_directory:
        other_args["working_dir"] = args.working_directory
    files = list_files(path_1, path_2, paths_id, **other_args)

    raise NotImplementedError


if __name__ == "__main__":
    main()
