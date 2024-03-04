import os
import grp

DEFAULT_FOLDER_PERMISSIONS = int("2770", base=8)  # the leading 2 is for the setgid bit.
DEFAULT_FILE_PERMISSIONS = int("0660", base=8)
