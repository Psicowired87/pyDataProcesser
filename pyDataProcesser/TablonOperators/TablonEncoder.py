
## Imports
import pandas as pd
import numpy as np

#from DataDictObject import *


class TablonEncoder:
    """TablonEncoder class is a class about the encoding of the whole dataset
    in order to be used in the analysis part. TablonEncoder is the class that
    develops the process specified in the encoderdictobject.
    It uses the instructions of the encoderdictobject to develope this task.
    This process has a memory in which is stored the input and the output
    variables.
    """
    # TODO: create a decode function.
    processname = 'encoder'

    def __init__(self, encoderdictobject):
        """The initialization of this class only requires the
        encoderdictobject.
        The class attributes are:
            - encoderdictobject: the operations needed to do.
            - memory: is the one which will store all the input and output
            variables we have in all the times used since it was initialized.
            [(inputvars, outputvars),...]
        """
        self.encoderdictobject = encoderdictobject
        self.memory = []

    def apply(self, dataframe, ordervars=[]):
        return self.encode(dataframe, ordervars)

    def encode(self, dataframe, orderedvars=[]):
        """Given the dataframe return the TablonDf and LabelDf dataframes.
        """
        ####### WARNING:
        # The input variables and the output could be totally different.
        ### There is a process of transformation.
        # 1. Transformation
        # 2. Renaming and filtering
        # 3. Filtering
        # 4. Splitting
        # 5. Setting an order.

        # 0. Setting process:
        if orderedvars:
            self.encoderdictobject.ordervars = orderedvars
        inputvars = list(dataframe.columns)
        # 1. Transformation, renaming and filtering and splitting
        #WARNING: hardcoded
        TablonDf, LabelDf =\
            self.encoderdictobject.transform_dataframe(dataframe,
                                                       ['var', 'label'])

        # TODO: Filter all the variables which have not statistical sense.
        # (All the values equal)
        # 2. Building the memory
        outputvars = list(TablonDf.columns)+list(LabelDf.columns)
        self.memory.append((inputvars, outputvars))
        return TablonDf, LabelDf


class Encoder:
    """Encoder class is a class about the encoding of the labels of a dataset
    in order to be used in the analysis part. Some functions are wrappers of
    encoding functions which belongs to other packages as sklearn.
    """
    # TODO: force initialize two things: variablename, method.
    def __init__(self, variable, method=[], function=None):
        """There are two ways of instantiation of this class. One is giving
        the name of the method and we will use one of the precoded methods.
        The other is to use a function. In this case we will evaluate the
        function into this class.
        """
        # Check and save variable
        if type(variable) == str:
            self.variablename = variable
        else:
            message1 = "Variable is not a string!! in the encoding. "
            message1 += "Variable is type: "
            message2 = str(type(variable))
            message = message1 + message2
            raise Exception(message)
        # Instantiation protocol:
        if function:
            self.codificationtype = 'personalized'
            self.function = function
            if method:
                self.method = method
            else:
                ### OJO: I do not know if it is implemented!!!
                # TODO: use __str__ of the function if you receive a function.
                self.method = function.str()
        else:
            if method:
                self.method = method
                self.codificationtype = method
            else:
                message1 = "WARNING: I am trying to"
                message1 += " encode the dataset and the variable "
                message2 = "'" + variable + "'"
                message3 = " do not have not a method neither a function "
                message3 += "specified"
                message = message1 + message2 + message3
                print(message)
                self.codificationtype = 'unitary_transformation'
        self.dictionary4values = {}
        # Wich maps the input variable with the output variables.
        # TODO: Have the possibility to
        self.dictionary4variables = {}
        self.output_variablenames = []
        # FUTURE: Have the possibility to receive two columns or more.
        # In order to do this it is possible that the variable input has to be
        # a list of variables. input_variablenames.

    # Receive only dataframe and variable if it is a change of name but by
    # default the same as always.
    # Error if it is not in the dataframe. OR Warning
    def encode(self, dataframe, variable=''):
        """There is the main function of encoding variable-wise."""
        if not variable:
            variable = self.variablename
        # TODO: Use variable and not self.variablename.
        # In order to do that you have to control: it is a string,
        # it is in the dataframe,
        # Reversible?? TO TEST
        if variable not in list(dataframe.columns):
            message = "The variable '" + variable
            message += "' you try to encode it is not in the dataframe"
            print(message)
            return np.array([])

        if self.codificationtype == '':
            # TODO: Temporal thing
            self.output_variablenames = [self.variablename]
            matrixarray = dataframe[self.variablename].as_matrix()
            matrixarray = self.shape_matrix(matrixarray, dataframe.shape[0])
            return matrixarray

        elif self.codificationtype == 'str2int':
            ## TODO: REPLACE with the output values
            #(when this functionality is implemented)
            self.output_variablenames = [variable]
            self.dictionary4values = {}
            keys = dataframe[variable].unique()
            for e in keys:
                try:
                    self.dictionary4values[e] = int(e)
                except:
                    self.dictionary4values[e] = 0     # value to be input maybe
            dataframe[variable] =\
                dataframe[variable].replace(self.dictionary4values)

            matrixarray = dataframe[self.variablename].as_matrix()
            matrixarray = self.shape_matrix(matrixarray, dataframe.shape[0])
            return matrixarray

        elif self.codificationtype == 'str2intbin':
            # In practique effects it is equal to the option below ... shit!
            # TODO: Temporal thing
            self.output_variablenames = [self.variablename]
            keys = list(set(dataframe[self.variablename]))
            try:
                values = np.array([k.lower() == 'true' for k in keys])
                values = np.int32(values)
            except:
                message = "The variable '" + variable
                message += "' have problem in the transformation to a"
                message += " boolean 0,1 coding"
                raise Exception(message)

            dictionary4values = dict(zip(keys, values))
            matrixarray = dataframe[self.variablename].map(dictionary4values)
            matrixarray = matrixarray.as_matrix()
            matrixarray = self.shape_matrix(matrixarray, dataframe.shape[0])
            return matrixarray

        elif self.codificationtype == 'cat2int':
            # TODO: Temporal thing
            self.output_variablenames = [self.variablename]
            keys = list(set(dataframe[self.variablename]))
            #ordered by alphabetic order
            #OJO: vars added a posteriori not follow the sorting criteria.
            keys.sort()
            values = range(len(keys))
            dictionary4values = dict(zip(keys, values))
            matrixarray = dataframe[self.variablename].\
                map(dictionary4values).as_matrix()
            matrixarray = self.shape_matrix(matrixarray, dataframe.shape[0])
            return matrixarray

        elif self.codificationtype == 'enum2vars':
            # temporaly dummy transformation   !!!!! TODO:
            self.output_variablenames = []
            matrixarray = np.mat([[] for i in range(dataframe.shape[0])])
            matrixarray = self.shape_matrix(matrixarray, dataframe.shape[0])
            return matrixarray

        # DANGER WITH THAT: Dummy method that need to be TESTED!!!
        # Create empty columns.
        elif self.codificationtype == 'unitary_transformation':
            self.output_variablenames = [self.variablename]
            matrixarray = np.mat(dataframe[self.variablename])
            matrixarray = self.shape_matrix(matrixarray, dataframe.shape[0])
            return matrixarray

        elif self.codificationtype == 'special_1_2_NS':
            self.output_variablenames = [self.variablename + '_1',
                                         self.variablename + '_2']
            dat = dataframe[self.variablename].apply(list)
            dat1 = dat.apply(lambda d: int('1' in d))
            dat2 = dat.apply(lambda d: int('2' in d))
            matrixarray1 = np.mat(dat1)
            matrixarray2 = np.mat(dat2)
            matrixarray = np.concatenate([matrixarray1, matrixarray2])
            matrixarray = self.shape_matrix(matrixarray, dataframe.shape[0])
            return matrixarray

        elif self.codificationtype == 'segm_pub_NetSales':
            self.output_variablenames = [self.variablename]
            dat = dataframe[self.variablename].apply(list)
            triggers = ['1', '7', '230', '231']
            dat = dat.apply(lambda d: int(bool([val for val in triggers
                                                if val in d])))
            matrixarray = np.mat(dat)
            matrixarray = self.shape_matrix(matrixarray, dataframe.shape[0])
            return matrixarray

        elif self.codificationtype == 'count':
            self.output_variablenames = [self.variablename]
            dat = dataframe[self.variablename].apply(list)
            dat = dat.apply(len)
            matrixarray = np.mat(dat)
            matrixarray = self.shape_matrix(matrixarray, dataframe.shape[0])
            return matrixarray

        else:
            self.output_variablenames = []
            matrixarray = np.mat(dataframe[self.variablename])
            matrixarray = self.shape_matrix(matrixarray, dataframe.shape[0])
            return matrixarray

    def decode(self, matrixarray):
        # Inverse of the dictionary
        # Problems with the created variables (degeneracy problem)
        return newmatrixarray

    def shape_matrix(self, matrix, nrows=0):
        """Function which shapes the array to a column matrix. It is coded in
        order to ALWAYS return a shape with more rows than columns.
        """
        matrix = np.mat(matrix).transpose()
        if nrows:
            if matrix.shape[0] == nrows:
                return matrix
            elif matrix.shape[1] == nrows:
                return matrix.transpose()
            else:
                shape = matrix.shape
                message = "WARNING: the output matrix we want to format in the"
                message += " encoding of the variable '" + self.variablename
                message += "' has the shape: " + str(shape)
                print(message)
                if shape[0] < shape[1]:
                    return matrix
                else:
                    return matrix.transpose()
        else:
            shape = matrix.shape
            if shape[0] < shape[1]:
                return matrix
            else:
                return matrix.transpose()
