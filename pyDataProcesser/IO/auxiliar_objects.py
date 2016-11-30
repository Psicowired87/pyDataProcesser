
"""
auxiliar objects
----------------
Definition of objects useful into that framework which could be standarize
to ease some tasks.

"""


class FileAddress:
    """This class define an object address to a file.
    """

    def __init__(self, filename, path):
        self.filename = filename
        self.path = path
        self.properties = FileInfo


class FileInfo:
    """This class it is used to define the specific information of a file.
    """

    def __init__(self, typeoffile, extension, separation, header):
        """This class contains several attributes.
                - typeoffile: {'structureddata','text',...}
                - extension: extension of the file. {'csv','xlsx','txt','',...}
                - separation: for structured data {',',';','\t'}
                - header: for structured data {True,False}
        """
        self.typeoffile = typeoffile
        self.extension = extension
        self.separation = separation
        self.header = header
