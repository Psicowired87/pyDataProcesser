# -*- coding: utf-8 -*-

"""
TablonReader
------------
This module contains the summarizing class of the parsing task.

"""

import csv
import os
import pandas as pd
import datetime
import codecs
import numpy as np

#from TransformationFunction import *
from ..DataDictObject import DataDictObject
from ..aux_functions import normalize_string, parse_header


class TablonReader:
    """It is a class which encapsulates the operation of parse a same
    structured datasets. The main function of this class is self.parse.
    In this class will transform a filename with its file path to a dataframe
    structure of python pandas package.
    It is done in order to be used more than one time with different files.
    In order to do this it is saved a memory of the input filepath and the
    output variables.
    It could happen that the files has different variables. The transformation
    used will be only apply the datadictobject given for the transformation.
    The different variables obtained will be stored in the memory.

    """
    processname = 'reader'

    # TODO: write_header in excel format
    # TODO: write_header in other formats
    # TODO: ods support
    # TODO: Automatic detection of delimiter
    # TODO: File and properties class?
    def _inititialization(self):
        # Cumulative memory
        self.memory = []    # [(filepath,[variables]),(...)...]

    def __init__(self, parserdictobject='', extension='xlsx', delimiter=';',
                 header=True):
        """This class has as class attributes:
            - File attributes: attributes which describes the properties of the
              files given by this client.
                * extension: the file type: {csv, xls, xlsx, ods, ...}
                * delimiter: the type of separator if it is a csv or txt file.
                             # In the future declare schema.
                * header : is a boolean variable which specifies if the file
                given has a specific header. If not it will write it.

        """
        ### TODO: Other type of files. By default: csv
        ### header is also part of the file attributes

        # Transformations Object
        if not parserdictobject:
            self.parserdictobject = DataDictObject('parser')
        else:
            self.parserdictobject = parserdictobject

        # File attributes:
        self.extension = extension
        self.delimiter = delimiter
        self.header = header

    ################################# Setters #################################
    ###########################################################################
    def set_parserdict(self, parserdictobject):
        """Set parserdict"""
        self.parserdictobject = parserdictobject

    def set_fileattributes(self, extension='xlsx', delimiter=';'):
        self.delimiter = delimiter
        self.extension = extension

    ############################# Main functions ##############################
    ###########################################################################
    def apply(self, filename='', pathfile=''):
        """"""
        return self.parse(self, filename, pathfile)

    def parse(self, filename='', pathfile=''):
        """The main function of this class. It is a wrapper of functions which
        parse.
        """
        ## 0. Control of inputs
        # Control the file
        if not filename:
            message = "Not a filename input. Please, input a correct filename."
            raise Exception(message)
        # Creation of the filepath:
        filepath = pathfile + filename
        # Control existence of the file:
        if not os.path.isfile(filepath):
            msg = "Not a file."
            msg += " I am not able to find the input file in the input path. "
            msg += "Please, input a correct filename or pathfile."
            raise Exception(msg)
        # Control the file is agree with the file attributes specified.
        #TODO: delimiter check
        extension = filename.split('.')[1]
        if extension != self.extension:
            message = "WARNING: "
            msg += "The file you want to parse is not the specified extension."
            msg += " This property will be reset."
            print(msg)
            self.extension = extension

        ## 1. Ensure header
        # Write a header if it is not header
        # TODO: Prepend header in each type of files.
        # TODO: Correct for filepath
        if not self.header:
            self.write_header(filename, pathfile, extension)

        ## 2. Parse the file with specific extension.
        if self.extension == 'xlsx':
            dataframe = self.parse_xlsx(filepath)
        #TODO:
        # More support to other type of files. All need to be xlsx until now.

        ## 3. Put in memory
        columns = list(dataframe.columns)
        self.memory.append((filepath, columns))
        return dataframe

    def parse_xlsx(self, filepath):
        """Main function to parse a xlsx file. You have to remember that this
        function only parse the sheet 1. It is preferable that the given file
        only has one sheet in order to avoid problems.
        """
        ## 1. Dealing with excel files.
        xl_file = pd.ExcelFile(filepath)
        dfs = dict(zip([(sheet_name, xl_file.parse(sheet_name))
                        for sheet_name in xl_file.sheet_names]))

        ## 2. Only get the first sheet.
        if len(xl_file.sheet_names) == 0:
            msg = "The excel file is empty. There is any sheet in the file."
            raise Exception(msg)
        elif len(xl_file.sheet_names) > 1:
            msg = "WARNING: There are more than one sheet. "
            msg += "The program only gets the first one."
            print(msg)
        dataframe = dfs[xl_file.sheet_names[0]]

        ## 3. Normalize the name of the variables
        dataframe.columns = [normalize_string(e) for e in dataframe.columns]

        ## 4. Applying the transformation
        dataframe = self.transformation(dataframe)
        return dataframe

    def transformation(self, dataframe, parserdictobject=''):
        """Returns the dataframe with expansions, filtering and ordering,
        returning the dataframe we want.
        """
        ## 1. Create an alternative parsing class..
        if not parserdictobject:
            parserdictobject = self.parserdictobject

        ## 2. Call the functions transform
        dataframe_tuple = parserdictobject.transform_dataframe(dataframe)

        ## 3. Ensure dataframe output.
        if type(dataframe_tuple) == tuple:
            dataframe = dataframe_tuple[0]

        return dataframe

    #################### Auxiliar to be recobered functions ###################
    ###########################################################################
    def set_fileidentity(self, filename='', pathfile=''):
        """Set properties a posteriori."""
        if filename != '':
            self.filename = filename
        if pathfile != '':
            self.pathfile = pathfile
        # QUESTION: Think about this attributes.
        # READ variables
        if pathfile != '' and filename != '':
            ## TODO: You have to input the whole path when this functionality
            # it is implemented.
            self.variables = parse_header(filename)
        if not self.allvariables:
            self.allvariables = self.variables
        self.delimiter = ';'

    def write_header(self, filename, pathfile, variables=[]):
        """This function writes the header if the function does not have.

        References
        ----------
        [1] .. http://stackoverflow.com/questions/4454298/prepend-a-line-to-an-
        existing-file-in-python

        """
        #if variables:
        # TODO: write_header(self, filename, pathfile)
        # TODO: header
        f = open('filename', 'r')
        temp = f.read()
        f.close()

        f = open('filename', 'w')
        f.write("#testfirstline")   # TODO

        f.write(temp)
        f.close()

    def parse_header(self, filename):
        """Function which reads only the first line to get the headers."""
        # TODO: external function
        with codecs.open(filename, 'rb') as csvfile:   # rU or rb
            datareader = csv.reader(csvfile, delimiter=';', quotechar='|')
            for row in datareader:
                variables = row
                break
        variables = [normalize_string(e) for e in variables]
        return variables

    def parse_csv(self, filename='', pathfile='', delimiter='',
                  parsingmode='pandas'):
        """This function is the one specialized in parsing csv files.
        """
        ###### It is outside: Probably it is better to delete this code.
        ### INITIATION OF THE NEEDED VARIABLES
        if not delimiter:
            delimiter = self.delimiter
        else:
            self.delimiter = delimiter
        # If the instantiation is uninformed of different specific feature.
        # Use the ones given in this function.
        #if not self.filename:
        #   self.filename = filename
        # Do the contrary. If not given filename, use the ones given by default
        if not filename:
            #filename = self.filename
            message = "Not a file. Please, input a correct filename."
            raise Exception(message)

        #### TODO: probably it is better do this out of this function
        # Go to the filepath
        if pathfile:
            os.chdir(pathfile)

        #######################################################################
        variablesfile = self.parse_header(filename)
        # Set variables from variables file
        # (if header and there is the first time to indicate file).
        if not self.variables:
            self.variables = variablesfile
            self.allvariables = self.variables
        if self.parserdictobject.transformationdict_list:
            #transformationdict_list[0]
            parserdict =\
                self.parserdictobject.build_transformationdict(0, False)
        else:
            parserdict = {}

        # Add new variables and functions in the cumulative parameters.
        #  (The memory)
        for newvar in variablesfile:
            if newvar not in self.allvariables:
                self.allvariables.append(newvar)
                ### TODO: OJOOOOOOOO !!!!!!!!!
                #self.allparserdict[newvar] = parserdict[newvar]
        ## PARSING ##
        # Pre-parsing:
        inpandas = True
        if parsingmode == 'pandas':
            try:
                dataframe = pd.read_csv(filename)
                dataframe.columns = [normalize_string(e)
                                     for e in dataframe.columns]
            except:
                inpandas = False
                dataframe = self.parse_manual_csv(filename, delimiter,
                                                  parserdict, variablesfile,
                                                  header)
        elif parsingmode == 'manually':
            inpandas = False
            dataframe = self.parse_manual_csv(filename, delimiter, parserdict,
                                              variablesfile, header)
        # Complete transformation of the dataset following the instructions
        # of the datadictobject
        # initialization of the puntero for the list of parsing dicts.
        i = 0 if inpandas else 1
        auxparserdictobject = self.parserdictobject
        auxparserdictobject.transformation_list =\
            self.parserdictobject.transformation_list[i:]
        # apply all the parsingdicts in the list sequencially.
        dataframe = self.transformation(dataframe)
        return dataframe

    def parse_manual_csv(self, filename, delimiter, parserdict, variablesfile,
                         header):
        """Parse all the file without the header"""
        ''' TODO: Give support for other type of files. Not only csv.'''
        # TODO: OJOOOOOO!!!!!!!!!
        # It calculates and return  parsercolsdict, parserdict
        # Check the size of the parserdict.
        msg = "There are more variables to parse that the actual variables in "
        msg += "the file. Revise it."
        if len(parserdict.keys()) > len(variablesfile):
            raise Exception(msg)
        # Create parsercolsdict = { colnumber: parserfunct} from cross
        # parserdict and variables or if is not given parserdict with name of
        # variables.
        parsercolsdict = {}
        if np.array(parserdict.keys()).dtype == 'int32':
            parsercolsdict = parserdict
            indices = np.array(parsercolsdict.keys())
            variables = list(np.array(variablesfile)[indices])
            parserdict = dict(zip(variables, parsercolsdict.values()))
        else:
            # Transform parserdict to parsercolsdict using variables
            # (remember that variables is an ordered list).
            for e in parserdict:
                parsercolsdict[variablesfile.index(e)] = parserdict[e]
        ######## REVISE
        lista = []
        with codecs.open(filename, 'rb') as csvfile:
            datareader = csv.reader(csvfile, delimiter=';', quotechar='|')
            # header
            datareader.next()
            for row in datareader:
                for i in parsercolsdict:
                    row[i] = parsercolsdict[i].applyparser(row[i])
                lista.append(row)

#        ############# DEBUG ######################
#        print(len(lista))
#        print(len(variablesfile))
#        print variablesfile
#        from collections import Counter
#        print(Counter([ len(e) for e in lista]))
#        ##########################################

        dataframe = pd.DataFrame(lista, columns=variablesfile)
        return dataframe

    def general_nullvalue_treatment(self, dataframe, to_operate=True,
                                    dictionaryfornulls={}):
        """General transformation for nulls values. It not consider individual
        treatment for any variable. TODO: Individual treatment for each
        variable.

        """
        if to_operate and not dictionaryfornulls:
            dataframe = dataframe.replace('\N', '')
            return dataframe
        elif not to_operate:
            return dataframe
        elif to_operate and dictionaryfornulls:
            for e in dictionaryfornulls:
                dataframe = dataframe.replace(e, dictionaryfornulls[e])
            return dataframe
        else:
            return dataframe

    def parse_dataframe(self, dataframe, parserdict):
        """In case it is possible to do it with dataframe"""
        # TODO:  Entrar SOLO strings en parserdict.
        # DEPRECATED
        msg = "WARNING: There are variables in the parserdict you give me "
        msg += "that are not in the column names."
        for e in parserdict:
            if e in list(dataframe.columns):
                #WARNING: No applyparser.
                dataframe[e] = dataframe[e].apply(parserdict[e].applyparser)
            else:
                print(msg)
        # TODO: introduce parameter to control this.
        # NULL TREATMENT
        # dataframe = self.general_nullvalue_treatment(dataframe)
        return dataframe

    def expand(self, dataframe, expansiondict, variablesfiltered=[]):
        """The aim of this function is the one to create from 1 variable more
        variables. The structure of the expansiondict is:
            {varname: [newvarname, functiontransformation]}

        Returns the dataframe expanded with the specified new variables.
        """
        # DEPRECATED
        # probable do it in other way???
        matrix = []
        newcols = []
        for var2trans in expansiondict:
            for e in expansiondict[var2trans]:
                # TODO: applyparser need to be corrected
                arraydf = dataframe[var2trans].apply(e[1].applyparser)
                # TODO: apply for expands and transformation of the columns.
                matrix.append(arraydf)
                newcols.append(e[0])
        # OJO!!!!!!! Could be that the matrix elements are not transposed.
        newdata = pd.concat(matrix, axis=1)
        newdata.columns = newcols
        #newdata = pd.DataFrame(matrix,columns = newcols)
        dataframe = pd.concat([dataframe, newdata], axis=1)

        if not variablesfiltered:
            variablesfiltered = list(dataframe.columns)
        dataframe = self.filter(dataframe, variablesfiltered)
        return dataframe

    def parse_row(self, row, parsercolsdict, variablesfile):
        """Transforms each row as it is specified in the parser functions

        TODO: improve control errors. What happens if both lists are not
        correct??
        """
        #################### DeprecationWarning ##############################
        for i in parsercolsdict:
            row[i] = parsercolsdict[i].applyparser(row[i])
        return row


class ParserFunction:
    """Functions to transform the data in the moment to be parsed.
    """
    def __init__(self, nameparser, parsingsnullstrategy='normal'):
        """
        TODO: Incorporate other variables, as format needed to parse a
        date ..., reference values as deliver date.
        TODO: Possibility to use external function
        """
        self.parser = nameparser
        self.parsingsnullstrategy = parsingsnullstrategy

        # No sabia donde ponerla.
        self.deliverday = datetime.datetime.strptime("16/06/2014", "%d/%m/%Y")

    def applyparser(self, text):
        """Function which parse each element of each column in order to return
        an object with the class and value wanted
        """
        # Missing values parsing strategy.
        if self.parsingsnullstrategy == 'normal':
            if text == '\\N':
                return ''

        # Parsing functions.
        if self.parser == 'specialbool_NetSales':
            # It has special characters which has to be converted to TRUE/FALSE
            if text == '\x01':
                return 'TRUE'
            else:
                return 'FALSE'

        elif self.parser == 'gender_NetSales':
            ''' It has the case of enumerate MALE, FEMALE. '''
            if len(text) > 10:
                return 'BOTH'
            return text

        elif self.parser == 'timefrom_NetSales':
            if text == '':
                return 0
            else:
            # TODO: deliverdate has to be out of here.
                day = datetime.datetime.strptime(text, "%d/%m/%Y")
                delta = (self.deliverday-day).days
                return 1./float(1+delta)

        elif self.parser == 'nameDomain_NetSales':
            domain = text.split('.')
            return domain[0]

        elif self.parser == 'enum2list':
            if text == '':
                return []
            text = text.lower()
            text = text.replace('\\N', '')
            text = text.replace('\n', '')
            text = text.replace(' ', '')
            return list(text.split(','))

        elif self.parser == '':
            return text
