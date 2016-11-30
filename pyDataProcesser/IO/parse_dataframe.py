
"""
Module oriented to parse dataframes. It is composed by a general functions
which acts as a switcher and generalized all the other parsing functions
specific of each extension.


TODO
----
Possible errors:
- Not a file in the pathfile
- Empty file (For excel for example)


Parser functions
- Input functions per column for the csv parser.
- Write and read header in other formats as excel

"""

import pandas as pd
import csv
import codecs
## TODO:
# import normalize_string *


def parse_dataframe(filepath, extension, name_data='', args=[]):
    """General function to parse structured databases. It acts as a switcher
    between all the individual functions of parsing each type of extension.

    Parameters
    ----------
    filepath: str or list
        the file path or filepaths of files we want to parse.
    extension: str or list
        extension of the file or files selected to be parsed.
    name_data: str or list
        the name of each dataframe of each file.
    args: list of dicts
        the needed variables for the parser of the selection

    Returns
    -------
    data: dict
        the data saved into a dict structure.

    """

    ## 0. Format variables and compute needed variables
    filepath = [filepath] if type(filepath) == str else filepath
    extension = [extension] if type(extension) == str else extension
    name_data = [name_data] if type(name_data) == str else name_data
    # Check lengths of each one
    assert(len(filepath) == len(extension))
    assert(len(filepath) == len(name_data))

    ## 1. Parsing the data
    data = {}
    for i in range(filepath):
        # Parsing file
        if extension == 'xlsx':
            aux = parse_xlsx(filepath[i], **args[i])
        elif extension == 'csv':
            aux = parse_csv(filepath[i], **args[i])
        # Saving the dataframes
        if type(aux) == dict:
            names_aux = aux.keys()
            name_d = [name_data[i]+'_'+e for e in names_aux]
            for j in range(len(names_aux)):
                data[name_d[j]] = aux[names_aux[j]]
        else:
            data[name_data[i]] = aux

    return data


###############################################################################
########################### Individual file parsers ###########################
###############################################################################
def parse_xlsx(filepath, sheets=None, normalize_str=True):
    """Main function to parse a xlsx file. It is preferable that the given file
    only has one sheet in order to avoid problems. It avoid the empty sheets.
    It could parse only the sheets we want to parse.

    Parameters
    ----------
    filepath: str
        path of the file.
    sheets: int or None
        the sheets we want to parse by number. Default None wich implies parse
        all the available sheets.
    normalize_str: boolean
        if we want to normalize the column names.

    Returns
    -------
    data: dict
        the dataframes loaded in memory.

    """

    ## 1. Dealing with excel files.
    xl_file = pd.ExcelFile(filepath)
    if sheets is None:
        sheet_names = xl_file.sheet_names
    else:
        sheet_names = xl_file.sheet_names[sheets]

    n = len(sheet_names)
    if len(xl_file.sheet_names) == 0:
        message = "The file is empty. It will return an empty variable."
        print message
        return {}

    ## 2. Get all the sheets in a dict with its names as a keys
    data = {}
    for i in range(n):
        try:
            # a. Parse
            aux = xl_file.parse(xl_file.sheet_names[i])
            # b. Normalize columns
            if normalize_str:
                aux.columns = [normalize_string(e) for e in aux.columns]
            # c. Save into the dict
            data[xl_file.sheet_names[i]] = aux
        except:
            pass

    return data


def parse_csv(filepath, delimiter='', parsingmode='pandas', header=True,
              normalize_str=True):
    """This function is the one specialized in parsing csv files. Acts as a
    switcher between the coded manual parser of csv files and the pandas
    parser.

    Parameters
    ----------
    filepath: str
        the filename with its path.
    delimiter: str
        the delimiter used to separate the different columns
    parsingmode: str, optional
        mode selected for parse the file.
    normalize_str: boolean
        if we want to normalize the column names.

    Returns
    -------
    dataframe: pd.DataFrame
        the data parsed and loaded in memory.

    """

    ## 0. Initialization of the variable needed

    ## 1. Parsing
    if parsingmode == 'pandas':
        try:
            dataframe = pd.read_csv(filepath)
            if normalize_str:
                cols = [normalize_string(e) for e in dataframe.columns]
                dataframe.columns = cols
        except:
            dataframe = parse_manual_csv(filepath, delimiter, header,
                                         normalize_str)
    elif parsingmode == 'manually':
        dataframe = parse_manual_csv(filepath, delimiter, header,
                                     normalize_str)

    return dataframe


def parse_manual_csv(filepath, delimiter=',', header=True, normalize_str=True):
    """Function which parse manually a csv file.

    Parameters
    ----------
    filepath: str
        the filename with its path.
    delimiter: str
        the delimiter used to separate the different columns
    header: boolean
        if it has header or not.
    normalize_str: boolean
        if we want to normalize the column names.

    Returns
    -------
    dataframe: pd.DataFrame
        the data parsed and loaded in memory.

    """

    ## 0. Prepare needed variables
    if header:
        variablenames = parse_header(filepath, delimiter, normalize_str)
    else:
        firstrow = parse_header(filepath, delimiter)
        variablenames = [str(e) for e in range(len(firstrow))]

    ## 1. Parsing method
    lista = []
    with codecs.open(filepath, 'rb') as csvfile:
        datareader = csv.reader(csvfile, delimiter=delimiter, quotechar='|')
        #header
        if header:
            datareader.next()
        for row in datareader:
            lista.append(row)
    dataframe = pd.DataFrame(lista, columns=variablenames)

    return dataframe


###############################################################################
############################# Auxiliary functions #############################
###############################################################################
def write_header(filepath, variables=[], delimiter=',', outfilepath=None):
    """This function writes the header if the function does not have.

    Parameters
    ----------
    filepath: str
        the filename with its path.
    variables: list
        variables we want to write in the file as a column names.
    delimiter: str
        the delimiter used to separate the different columns.
    outfilepath: str
        the filename with the path of the output file.

    """
    # TODO: header = http://stackoverflow.com/questions/4454298/
    # prepend-a-line-to-an-existing-file-in-python
    if outfilepath is None:
        outfilepath = filepath

    if variables != []:
        f = open(filepath, 'r')
        temp = f.read()
        f.close()
        f = open(outfilepath, 'w')
        firstline = delimiter.join(variables)+'\n'
        f.write(firstline)
        firstline = ''
        f.write(temp)
        f.close()


def parse_header(filename, delimiter, normalize_str=True):
    """Function which reads only the first line to get the headers.

    Parameters
    ----------
    filepath: str
        the filename with its path.
    delimiter: str
        the delimiter used to separate the different columns.
    normalize_str: boolean
        if we want to normalize the column names.

    Returns
    -------
    variables: list
        variables we want to write in the file as a column names.

    """
    with codecs.open(filename, 'rb') as csvfile:   # rU or rb
        datareader = csv.reader(csvfile, delimiter=delimiter, quotechar='|')
        for row in datareader:
            variables = row
            break
    if normalize_str:
        variables = [normalize_string(e) for e in variables]
    return variables
