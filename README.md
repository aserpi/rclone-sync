rclone-sync

# Exit codes
As usual, `0` is the only successful exit code.

## Path errors (0x)
- `1` invalid path_1
- `2` invalid path_2
- `3` path_1 and path_2 are identical
- `4` cannot access path_1
- `5` cannot access path_2

## rclone errors (1x)
- `10` cannot find `rclone` executable
- `11` cannot use rclone configuration file

## rclone-sync errors (2x)
- `21` cannot access path_1's database
- `22` cannot access path_2's database
- `23` a lock file for the paths is already in place
- `24` invalid working directory