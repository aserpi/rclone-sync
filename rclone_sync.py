"""Bi-directional sync for rclone.

For more info: https://github.com/aserpi/rclone-sync
"""
import argparse
import atexit
import hashlib
import pathlib
import sys
import tempfile

__version__ = "0.0.1"


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


def main() -> None:
    """Main function."""
    parser = argparse.ArgumentParser(description="Bi-directional sync for rclone.")

    # Positional arguments
    parser.add_argument("path_1", help="first path")
    parser.add_argument("path_2", help="second path")

    # Optional arguments
    parser.add_argument("-V", "--version", action="version", version=f"%(prog)s v{__version__}")

    args = parser.parse_args()

    path_1 = args.path_1
    path_2 = args.path_2

    # Since path order is important, using always the same (for
    # different runs) greatly simplifies application logic.
    if path_1 > path_2:
        path_1, path_2 = path_2, path_1
    paths_id = get_paths_id(path_1, path_2)

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
