rclone-sync

# Exit codes
As usual, `0` is the only successful exit code.

## Path errors (0x)
- `1` invalid path_1
- `2` invalid path_2
- `3` path_1 and path_2 are identical
- `4` a lock file for the paths is already in place
- `5` invalid working directory

## rclone configuration errors (1x)
- `10` cannot find `rclone` executable
- `11` cannot use rclone configuration file
