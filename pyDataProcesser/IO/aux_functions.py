
"""
Auxiliary functions.
"""

import os


def get_extension_file(filepath):
    fileName, fileExtension = os.path.splitext(filepath)
    return fileExtension[1:]
