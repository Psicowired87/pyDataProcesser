# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import unicodedata
#from functions import *
#from datetime import datetime
from aux_functions import normalize_string, control_input,\
    codification_precoded_functions, dataframewise_precoded_function,\
    general_precoded_functions

# TODO: Transform precoded functions
# TODO: Implement support for own functions


class TransformationFunction:
    """This class is the one which agglutinates all the classes related with
    transform a dataset to another dataset
        To be used in the call to SupportFunction
            * self.transformationtype
            * self.function_descriptor
        Input outputs: it will be corrected during the running for the sake of
        consistence.
            * self.input_variable_list
            * self.output_variable_list
        The real function. It has the apply function. You input dataframe and
        it returns dataframe.
            * self.function

    """

    def __init__(self, input_variable_list, output_variable_list=[],
                 transformationtype='', function_descriptor=''):
        """There are a lot of possible types of transformations of the data
        regarding not only the input-output number of variables but also if
        it is dependant of the other values in the dataset.
        In order to make the difference we are going to classify the possible
        transformation and indicate in the transformationtype variable.

            The possible types (transformationtype) have to be selected among
            these options:
                * 'general': this type of transformation functions are
                the ones which are applied to each value of the dataset,
                element-wise, so, regardless the values of the other elements.
                * 'codification': this transformation is like the
                'dataframe-wise' but it is implemented to not change the
                relative differences between the values in the data.
                e.g. in this type is not allowed to collapse 2 different
                values in the column. It works under the assumption we only
                want to change the data to
                * 'dataframe-wise': this transformation is applied to a columns
                in order to obtain another columns. It could be use to codify
                the data but it is not the best option.

            We have to know also the the input_variable_list and
            output_variable_list which describes the names of the columns
            selected from the whole dataframe in order to operate them,
            and the output names of the dataframe obtained after operate
            the input dataframe.

            We also need to know the function which could be indicated in
            different types:
                * str: If it is string we want a precoded function.
                * dict: It could be 2 types of dict structure:
                    {variables: {to_replace: replace}} & {to_replace: replace}
                * handle function: If we pass a function coded by ourself out
                of the structure of this module.
        """

        # initializing the variables directly
        self.transformationtype = transformationtype
        if not transformationtype:
            self.transformationtype = 'general'
        self.input_variable_list =\
            [normalize_string(e) for e in input_variable_list]
        self.output_variable_list =\
            [normalize_string(e) for e in output_variable_list]
        self.function_descriptor = function_descriptor

        # Controlling "codification" requisites
        logi_codi = transformationtype == 'codification'
        logi = len(self.output_variable_list) != len(self.input_variable_list)
        if logi_codi and not self.output_variable_list:
            self.output_variable_list = self.input_variable_list
        # allow codification in groups: required condition:
        # same number of elements (it will be done respecting the order)
        if logi_codi and logi:
            msg = "Different length of the list of input and output variables "
            msg += "in a codification type transformation"
            raise Exception(msg)

    def apply(self, dataframe):
        """This function performs the transformation of the data as it is
        specified in the instantiated class. It is a conservative function.
        It uses the self.output_variable_list as a indicator of the number
        of variables to be output.

        """
        # Control of the input-output variables.
        if_gen = self.transformationtype == 'general'
        if not self.output_variable_list and not if_gen:
            message = "WARNING: Involuntary filtering the data in the "
            message += "TransformationFunction"
            print(message)
            return pd.DataFrame()

        columns = list(dataframe.columns)
        if not self.output_variable_list and if_gen:
            self.input_variable_list = columns
        if_nl = len(self.output_variable_list) != len(self.input_variable_list)
        if not self.output_variable_list or if_nl:
            self.output_variable_list = columns

        enter_indataframe, enter_notindataframe, col_notenter =\
            control_input(self.input_variable_list, columns, (2, 0))
        #message = "Problems with the variables. Input_variable_list and
        #enter_indataframe are not the same."
        #message = "WARNING: The variables " + str(enter_notindataframe)
        #+ " are not present in the dataframe"

        ninvar = len(self.input_variable_list)
        noutvar = len(self.output_variable_list)
        ### The 'general' type
        ### The 'codification' type
        if_codi = self.transformationtype == 'codification'
        if_gen = self.transformationtype == 'general'
        if_ll = len(self.input_variable_list) < len(self.output_variable_list)
        if_ml = len(self.input_variable_list) > len(self.output_variable_list)
        if if_codi or if_gen:
            #the codification is done column-wise.
            #So, the number of input and output variables has to be equal.
            if len(self.input_variable_list) == len(self.output_variable_list):
                self.function = SupportFunction(self.function_descriptor,
                                                (ninvar, ninvar),
                                                self.transformationtype)
                out_dataframe = self.function.\
                    apply(dataframe[self.input_variable_list],
                          self.output_variable_list)
            #If it is more output it transforms all the input and we select the
            #first one names in order.
            elif if_ll:
                message = "WARNING: There are more output variables than input"
                message += " ones in the codification transformation type. "
                message += "They should have equal size."
                message += "\n" + "Input variables: "
                message += str(self.input_variable_list)
                message += "\n" + "Output variables: "
                message += str(self.output_variable_list)
                print(message)
                self.function = SupportFunction(self.function_descriptor,
                                                (ninvar, ninvar),
                                                self.transformationtype)
                out_dataframe =\
                    self.function.apply(dataframe[self.input_variable_list],
                                        self.output_variable_list[:ninvar])
            # If it is more input it only transforms the first ones in order.
            elif if_ml:
                message = "WARNING: There are more input variables than output"
                message += " ones in the codification transformation type. "
                message += "They should have equal size."
                message += "\n" + "Input variables: "
                message += str(self.input_variable_list)
                message += "\n" + "Output variables: "
                message += str(self.output_variable_list)
                print(message)
                self.function = SupportFunction(self.function_descriptor,
                                                (noutvar, noutvar),
                                                self.transformationtype)
                out_dataframe =\
                    self.function.\
                    apply(dataframe[self.input_variable_list[:noutvar]],
                          self.output_variable_list)

        ### The 'dataframe-wise' type
        elif self.transformationtype == 'dataframe-wise':
            #no restriction of number of input-output vars.
            self.function = SupportFunction(self.function_descriptor,
                                            self.input_output_number_vars(),
                                            self.transformationtype)
            out_dataframe =\
                self.function.apply(dataframe[self.input_variable_list],
                                    self.output_variable_list)
        self.output_variable_list = list(out_dataframe.columns)
        return out_dataframe

    def re_apply(self, dataframe, output_variable_list=[]):
        """This functions has the aim to reuse the functions initialized
        in the apply function in order to reuse for different dataset
        respecting the parameters set, during the first study of the data.
        """
        if not self.function:
            message = "There it is needed to use apply function before."
            raise(message)
        if not output_variable_list:
            output_variable_list = self.output_variable_list
        dataframe = self.function(dataframe, output_variable_list)

    def input_output_number_vars(self):
        return len(self.input_variable_list), len(self.output_variable_list)


class SupportFunction:
    """This class makes a support to the TransformationFunction in order to
    clarify and save the problems related with dealing with different inputs
    as:
    * strings, dictionaries and own functions.

    From outside the most interesting issue to use from this class is the apply
    function.
    The others are for the internal use preferentially.

    """
    # TODO: replace in the strings not in the dataframe.

    def __init__(self, function, input_output_number_vars=(1, 1),
                 transformationtype = 'general'):
        """The initialization of the support function is done by three
        different ways:
            * type(function) == str
            * type(function) == dict
                2 types of dict
                    - (dicts of dicts){variables: {to_replace: replace}}
                    - (dicts){to_replace: replace}
            * type(function) == function
            TODO:  ( try to apply to a sample dataframe)
            ¡¡¡¡NEED TO BE IMPLEMENTED!!!!

        """

        if type(function) == str:
            self.functionname = function
            self.functiontype = 'precoded'
        elif type(function) == dict:
            self.functionname = 'replace_' + str(function)
            self.functiontype = 'replace'
            self.replace_dict = function
        else:
            self.functionname = ''
            self.functiontype = 'own_function'
            self.function = function
            # TODO: implement properly and test with a dataset

        self.transformationtype = transformationtype

        if self.transformationtype == 'codification':
            self.dictionary4values = {}

        # TODO:
        self.input_output_number_vars = input_output_number_vars

    def apply(self, input_dataframe, output_variables):
        """Function which transforms the input dataframe (dataframe only with
        the variables which have to be transformed) to the output_dataframe.
        """

        # 0. Control input
        # control estimated output
        if len(output_variables) != self.input_output_number_vars[1]:
            self.input_output_number_vars =\
                (self.input_output_number_vars[0], len(output_variables))
            message = "WARNING: There are different number of variables than "
            message += "the ones specified as the output of the function "
            message += self.functionname
            print(message)

        # control real input
        if input_dataframe.shape[1] != self.input_output_number_vars[0]:
            self.input_output_number_vars =\
                (input_dataframe.shape[1], self.input_output_number_vars[1])
            message = "WARNING: There are different number of variables than "
            message += "the ones specified as the input of the function "
            message += self.functionname
            print(message)

        # 1. Transformation
        if self.functiontype == 'precoded':
            if self.transformationtype == 'general':
                output_dataframe =\
                    general_precoded_functions(self.functionname,
                                               input_dataframe)
            elif self.transformationtype == 'codification':
                self.dictionary4values =\
                    codification_precoded_functions(self.functionname,
                                                    input_dataframe,
                                                    self.dictionary4values)
                output_dataframe =\
                    input_dataframe.replace(self.dictionary4values)
            elif self.transformationtype == 'dataframe-wise':
                output_dataframe =\
                    dataframewise_precoded_function(self.functionname,
                                                    input_dataframe)
        elif self.functiontype == 'replace':
            output_dataframe = input_dataframe.replace(self.replace_dict)
        elif self.functiontype == 'own_function':
            if self.transformationtype == 'general':
                output_dataframe = input_dataframe.apply(self.function)
            elif self.transformationtype == 'dataframe-wise':
                output_dataframe = self.function(input_dataframe)

        # 2. Control real output
        if output_dataframe.shape[1] < len(output_variables):
            msg = "WARNING: There are different number of variables than "
            msg += "the ones specified"
            print(msg)
        elif output_dataframe.shape[1] > len(output_variables):
            msg = "There are different number of variables than the ones "
            msg += "specified"
            raise Exception(msg)

        output_variables = output_variables[:output_dataframe.shape[1]]

        output_dataframe.columns = output_variables
        return output_dataframe
