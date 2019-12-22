"""Bi-directional sync for rclone.

For more info: https://github.com/aserpi/rclone-sync
"""
import argparse
import sys

__version__ = "0.0.1"


def main() -> None:
    """Main function."""
    parser = argparse.ArgumentParser(description="Bi-directional sync for rclone.")

    # Optional arguments
    parser.add_argument("-V", "--version", action="version", version=f"%(prog)s v{__version__}")

    args = parser.parse_args()

    raise NotImplementedError


if __name__ == "__main__":
    main()
