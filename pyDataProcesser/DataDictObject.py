# -*- coding: utf-8 -*-

#from TransformationFunction import *
#from functions import *


class DataDictObject:
    """This class is the one related with the storing of the information
    related with the data processing.
    There are three types of possible structures to be stored:
        * transformationdict: is the transformation 1 to 1 of the variables.
        * expansiondict: is the creation of  new features from a given ones.
                {var: [[newvar, method ],...,[newvar, method ]]}
        * transformationtuples_list_raw:
            it is a list of tuples. Each tuple represents a transformation
            operation, implemented by only one TransformationFunction.
                    [([inputvarlist],[outpuvarlist],transformationtype, function),(),....]
        * transformationtuples_list:
            it is a list of list of TransformationFunction.
                [[TransformationFunction,...],...]
        * filteringdict:
            it is a dict in which we can filter data but its main function is
            renaming variables.
        * typevariablesdict:
            it defines different types of variables in which we can split the
            dataframe.
        * ordervars:
            it is a list of the expected order of the given results.

    There are another structures not directly related with the purpose of
    this class but can help in other processes.

    TODO:
    -----
    In order to developed this each transformationfunction (function n1 to n2)
    has to offer the information of the values of n1 and n2.
    Also it is interesting to have to opportunity to obtain the enter class
    of the transformationfunction and the output class.
    """

    def __init__(self, processtype=''):
        """The initialization of this object only requires the specification
        of the type of transformation information required.
        The variable involved is the processtype and can be set in these
        different values:
            * general: not specified
            * parser: the one which manage the RawData to Tablon transformation.
            * format: the one which manage the Tablon to Dataframe transformation.
            * encoder: the one which manage the Tablon to Matrixs transformation.
            * loader: the one which manage the Tablon to DB transformation.
            * preexploratory: the one which manage the to exportable pdf.
            * exploratory : the one which manage the Matrixs to exportable pdf.
            It is only for information purpose.
        """
        # TOTEST: Empty datadictobject.
        if not processtype:
            processtype = 'general'
        self.processtype = processtype
        self.transformationtuples_list_raw = []
        self.transformationtuples_list = []
        self.filteringdict = {}
        self.typevariablesdict = {}
        self.ordervars = []  # with the output variables

    ####################### Input of batch information ########################
    ###########################################################################
    def set_information_dicts(self, transformationtuple_list=[],
                              filteringdict={}, typevariablesdict={},
                              ordervars=[]):
        """This function centralize the function of the initialization and
        settting of the given variables.

        """
        # 0. Control of the type of inputs.
        # list of tuples (1 element) or list of lists.
        if not type(transformationtuple_list) == list:
            message = "The filteringdict used do not have the correct "
            message += "structure. It is not a list."
            raise Exception(message)
        else:
            if all([type(e) == list for e in transformationtuple_list]):
                transformationtuple_list = transformationtuple_list
            elif all([type(e) == tuple for e in transformationtuple_list]):
                transformationtuple_list = [transformationtuple_list]
            else:
                message = "Something is wrong with the "
                message += "transformationtuple_list input."
                raise Exception(message)

        if not type(filteringdict) == dict:
            message = "The filteringdict introduced is not a dictionary."
            raise Exception(message)
        if not type(typevariablesdict) == dict:
            message = "The typevariablesdict introduced is not a dictionary."
            raise Exception(message)
        if not type(ordervars) == list:
            message = "The ordervars introduced is not a list."
            raise Exception(message)

        # 1. Input:
        for trans in transformationtuple_list:
            self.add_transformationtuple(trans)
        self.set_filteringdict(filteringdict)
        self.set_typevariablesdict(typevariablesdict)
        self.set_ordervars(ordervars)

    def set_datadictobject(self, datadictobject):
        """This function allows us to reset an initialized datadictobject.
        This transforms the initialized datadictobject to a copy of the given
        datadictobject.
        """
        self.processtype = datadictobject.processtype
        self.transformationtuples_list =\
            datadictobject.transformationtuples_list
        self.transformationtuples_list_raw =\
            datadictobject.transformationtuples_list_raw
        self.filteringdict = datadictobject.filteringdict
        self.typevariablesdict = datadictobject.typevariablesdict
        self.ordervars = datadictobject.ordervars
        self.expectedknown_inputvariables =\
            datadictobject.expectedknown_inputvariables
        self.expectedknown_outputvariables =\
            datadictobject.expectedknown_outputvariables

    def add_transformationtuple(self, transformationtuple):
        """This function adds to the list of transformationtuples_list a new
        transformationtuple. Transformationtuple is a list of tuples.
        """
        # 0. Control type of input:
        if not type(transformationtuple) == list:
            message = "WARNING: The transformationtuple used do not have the "
            message += "correct structure. It is not a list"
            raise Exception(message)
        if not all([type(e) == tuple for e in transformationtuple]):
            message = "WARNING: There are some elements in the list that have "
            message += "not the correct structure. They have to be a tuple."
            raise Exception(message)
        if not all([len(e) == 4 for e in transformationtuple]):
            message = "WARNING: There are some elements in the list that have "
            message += "not the correct structure. "
            message += "They have to be a tuple of 4 elements."
            raise Exception(message)
        # 1. NORMALIZATION of the strings we input.
        # The titles of the variables has to be unicode utf-8 and lowercase
        # letters without strings at the end.
        for i in range(len(transformationtuple)):
            ls0 = [normalize_string(e) for e in transformationtuple[i][0]]
            ls1 = [normalize_string(e) for e in transformationtuple[i][1]]
            transformationtuple[i] = (ls0, ls1, transformationtuple[i][2],
                                      transformationtuple[i][3])
        self.transformationtuples_list_raw.append(transformationtuple)
        # 2. Initialize the functions.
        transformationtuples_list = []
        for e in transformationtuple:
            transformationtuples_list.append(TransformationFunction(e[0], e[1],
                                                                    e[2], e[3])
                                             )
        self.transformationtuples_list.append(transformationtuples_list)

    def set_filteringdict(self, filteringdict):
        """This function replace the filteringdict stored before.
        The functions which uses this filteringdict will raise a warning if
        there is some variables not in the dataframe.
        The structure of the filtering dict is the type:
            {variable_input: variable_ouput}

        """
        # 0. Control type of input.
        if not type(filteringdict) == dict:
            message = "WARNING: The filteringdict used do not have the correct"
            message += "structure. It is not a dictionary"
            raise Exception(message)

        # 1. NORMALIZATION of the strings we input.
        # The titles of the variables has to be unicode utf-8 and lowercase
        # letters without strings at the end.
        # if filteringdict[e] != '']
        keys = [normalize_string(e) for e in filteringdict.keys()]
        # if filteringdict[e] != '']
        values = [normalize_string(e) for e in filteringdict.values()]
        self.filteringdict = dict(zip(keys, values))  # TO DEBUG

        # 2. Control the output variables . %%%% TODO:
        try:
            self.expectedknown_outputvariables =\
                [filteringdict[e] for e in filteringdict
                 if filteringdict[e] != '']
        except:
            message = "Please, add the dictionaries in order. "
            message += "There are some variables you try to filter that there "
            message += "are not in the specific variables"
            raise Exception(message)

    def set_typevariablesdict(self, typevariablesdict):
        """This function sets the dictionary which describes the type of
        function of the named variable.
        The main function of this dictionary is the hability to split the
        dataset in different parts regarding the type of the variables.

        """
        # 0. Control type of input
        if not type(typevariablesdict) == dict:
            m = "The typevariablesdict given is not a dict as it is required."
            raise Exception(m)

        # 1. NORMALIZATION of the strings we input.
        # The titles of the variables has to be unicode utf-8 and lowercase
        #letters without strings at the end.
        keys = [normalize_string(e) for e in typevariablesdict.keys()]
        values = typevariablesdict.values()
        self.typevariablesdict = dict(zip(keys, values))

    def set_ordervars(self, ordervars):
        """This function sets the preferred order of the variables of the
        output dataframe.
        """
        # 0. Control type of input:
        if not type(ordervars) == list:
            message = "The ordervars given is not a list."
            raise Exception(message)

        # 1. NORMALIZATION of the strings we input.
        # The titles of the variables has to be unicode utf-8 and lowercase
        # letters without strings at the end.
        self.ordervars = [normalize_string(e) for e in ordervars]

    def transform_dataframe(self, dataframe, splittervartype=[]):
        """Generic function for transform a dataframe.
        """
        # 0. Control and Initializing
        # 1. Applying transformations
        if self.transformationtuples_list:
            for phase in self.transformationtuples_list:
                for transfunct in phase:
                    dataframe[transfunct.output_variable_list] =\
                        transfunct.\
                        apply(dataframe[transfunct.input_variable_list])
                    # warning: there is no way for the output_variable_list.
        # 2. Filter
        dataframe = self.filter(dataframe, self.filteringdict)

        # 3. Split
        dataframe_tuple = self.split(dataframe,
                                     self.typevariablesdict,
                                     splittervartype)
        #print(dataframe_tuple[0].columns)
        #print(self.ordervars)
        # 4. Reorder
        dataframe_tuple = self.reorder(dataframe_tuple, self.ordervars)
        #print(dataframe_tuple[0].columns)
        return dataframe_tuple

    def filter(self, dataframe, filtering, ordervars=[]):  # TOFINISH
        """Generic function for filtering a dataframe. There are different
        possible ways to indicate the way of filtering that this function
        accepts.
        The possible filtering element is:
            * dict: the dict option has the structure {varname: newvarname}.
            If the newvarname is '' this variable is filtered.
            If a given variable not appears, it is considered that it will
            be not renamed but it will not be filtered.
            // * list: there is the list of final variables we want.
            // (SI QUIERES METER LISTAS ADAPTALO EN set_filteringdict)
            Returns the dataframe filtered, renamed and ordered.

        """

        # Initialization
        filteringdict = self.filteringdict
        corrected_filteringdict = self.filteringdict
        if not ordervars:
            ordervars = self.ordervars
        columns = dataframe.columns

        # TODO: possible control function ?????????:
        # intersectvars = control_function(columns=columns,
        #                                  possiblevars=self.db_vars)
        #Add to the filtering the variables that are in the dataframe but there
        #are not in the filteringdict. This variables are not filtered.
        # Do not change their names.
        oldvars = [e for e in columns if e not in filteringdict.keys()]
        for e in oldvars:
            corrected_filteringdict[e] = e

        # Alert for the vars given that there are not in the dataframe.
        lackvars = [e for e in filteringdict.keys() if e not in columns]
        if lackvars:
            message = "WARNING: The following variables we want to filter are "
            message += "alredy not in the dataframe you want to be filtered: "
            for e in lackvars:
                message = message + e + ", "
            message = message[:-2]
            print(message)
        # Delete from the dict the variables that they do not have a name for
        #the output filteringdict.
        filtered_vars_list = [e for e in filteringdict.keys()
                              if filteringdict[e] == '']
        for e in filtered_vars_list:
            del corrected_filteringdict[e]
        # Filter:
        dataframe = dataframe[corrected_filteringdict.keys()]
        # Rename
        dataframe.columns = corrected_filteringdict.values()
        return dataframe

    def split(self, dataframe, typevariablesdict, splittervartype=[]):
        """This is a generic function for carry out the task of split
        a dataframe. In order to do that we need:
            * typevariablesdict: which is a dict in which we specify the
            * splittervartype: it could be use to specify the variables
            we want to use the split

        """
        # 1. Obtain splitlist
        if not splittervartype:
            splitlist = [list(dataframe.columns)]  # typevariablesdict.keys()
        else:
            splitlist = [[e1 for e1 in typevariablesdict.keys()
                          if typevariablesdict[e1] == e2]
                         for e2 in splittervartype]

        # 2. Dataframes
        dataframe_list = []
        for e in splitlist:
            intersected_vars, lack_vars, absent_vars =\
                control_input(list(dataframe.columns), e, (0, 1))
            dataframe_list.append(dataframe[intersected_vars])

        dataframe_tuple = tuple(dataframe_list)

        return dataframe_tuple

    def reorder(self, dataframe_tuple, ordervars=[]):    # TOTEST
        """This function has its aim utility, to reorder the dataframe outputs
        as we specify in ordervars.
        """

        # 0. Control of the input
        if type(dataframe_tuple) == tuple:
            n = len(dataframe_tuple)
                #TODO: Control if the tuple has pd.DataFrames
        elif type(dataframe_tuple) == pd.DataFrame:
            dataframe_tuple = (dataframe_tuple)
            n = 1
        if not ordervars:
            ordervars = [list(e.columns) for e in dataframe_tuple]

        if type(ordervars[0]) != list:
            ordervars = [ordervars]

        # 1. Filtering with order
        dataframe_list = []
        for i in range(n):
            ordervars[i], lack_vars, absent_vars =\
                control_input(list(dataframe_tuple[i].columns),
                              ordervars[i], (1, 0))
            dataframe_list.append(dataframe_tuple[i][ordervars[i]])
        dataframe_tuple = tuple(dataframe_list)
        return dataframe_tuple
