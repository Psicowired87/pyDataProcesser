
"""
Auxiliar functions
------------------
Collection of main auxiliar functions.

"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import unicodedata


########################### Administrative functions ##########################
###############################################################################
def control_input(input_vars, possible_output_vars, control_alert=(0, 0)):
    """There are different possibilities and combinations.
    It could happen that:
        - [lack_vars]: There are input_vars which are not in
        possible_output_vars. They will be filtered out, and display a warning.
        Sure a warning here?
        - [absent_vars] There are possible_output_vars which are not in
        input_vars. They will be avoided, and display a warning.
        - [intersected_vars]: There are input_vars which are also in
        possible_vars. These ones are the ones will be returned.
    In order to be adaptable to the differents needings we have the possibility
    to write WARNINGS or raise EXCEPTIONS.
    The codes used are in control_alert:
        - control_alert[0] : flag for the lack_vars.
        - control_alert[1] : flag for the absent_vars.
    The flags specified above could have the different values:
        * 0: There is no alert for the given flag.
        * 1: It gives a Warning for the variables related with the flag.
        * 2: It raises an Eception for the variables related with the flag.

    """
    # TODO: syslog or sysmessage

    # 1. Calculation of the list of all possibilities.
    intersected_vars = [e for e in possible_output_vars if e in input_vars]
    lack_vars = [e for e in input_vars if e not in possible_output_vars]
    absent_vars = [e for e in possible_output_vars if e not in input_vars]

    # 2. Message displaying
    if control_alert[0] == 1:
        if lack_vars:
            msg = "WARNING: The following input variables we want to filter "
            msg += "are not in the possible output variables you want to be "
            msg += "filtered: "
            for e in absent_vars:
                msg = msg + e + ", "
            msg = msg[:-2]
            print(msg)
    elif control_alert[1] == 1:
        if absent_vars:
            msg = "WARNING: The following variables we want to obtain after "
            msg += "the filtering are not in the input variables: "
            for e in absent_vars:
                msg = msg + e + ", "
            msg = msg[:-2]
            print(msg)
    elif control_alert[0] == 2:
        if lack_vars:
            msg = "There are some input variables which there are not in the "
            msg += "expected output variables. These variables are: "
            for e in absent_vars:
                msg = msg + e + ", "
            msg = msg[:-2]
            raise Exception(msg)
    elif control_alert[1] == 2:
        if absent_vars:
            msg = "There are some expected output variables which there are "
            msg += "not in the input variables. These variables are: "
            for e in absent_vars:
                msg = msg + e + ", "
            msg = msg[:-2]
            raise Exception(msg)

    return intersected_vars, lack_vars, absent_vars


def ensure_variable_existance(default_dictionary):
    """This function checks if all the names given are present in your
    workspace.
    If some of this function does not exist it will be set to a default value.
    """
    # TODO: for future
    print(dir())
    print(globals().keys())
    print(locals().keys())
    for e in default_dictionary:
        if not e in globals():
            exec("global " + e)
            exec(e + ' = ' + str(default_dictionary[e]))


###################### Plotting and statistic functions  ######################
###############################################################################
### plots for categoric vars
def calculate_description_variable(dataframe, variablename, ydata):
    """
    """
    #variablename = list(dataframe.columns)[0]
    values = list(dataframe[variablename].unique())
    conversion = []
    totals = []
    for e in values:
        conversion.append(float(ydata[dataframe == e].mean()))
        totals.append(int(ydata[dataframe == e].count()))
    #return values, totals, conversion
    #initialization plot
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    # location bars
    ind = np.arange(N)
    bars = ax.bar(ind, conversion)
    # ax1 = figure of bars or histogram
    # Labels
    ax1.set_ylabel('Number of leads')
    # Duplication
    ax2 = ax1.twinx()
    ax2.plot(x, y2, 'r')
    title = "Univariate plot of variable '" + variablename + "'"
    ax2.set_title()
    ax2.legend((bars[0], lines[0]), ('Totals', 'Conversion'))
    return fig


def calculate_barplot(dataframe, variablename):
    """
    """
    #variablename = list(dataframe.columns)[0]
    values = list(dataframe[variablename].unique())
    totals = []
    for e in values:
        totals.append(int(ydata[dataframe == e].count()))
    #return values, totals, conversion
    #initialization plot
    fig = plt.figure()
    ax = fig.add_subplot(111)
    # location bars
    ind = np.arange(N)
    bars = ax.bar(ind, conversion)
    # ax1 = figure of bars or histogram
    # Labels
    ax1.set_ylabel('Number of leads')
    # Duplication
    ax2 = ax1.twinx()
    ax2.plot(x, y2, 'r')
    title = "Univariate plot of variable '" + variablename + "'"
    ax2.set_title()
    ax2.legend((bars[0]), ('Totals'))
    return fig


######################### Auxiliar filter functions  ##########################
###############################################################################
def general_precoded_functions(methodname, dataframe):
    """This is a function of functions. The conditions of these functions are:
        * The input of the function has to be a individual object.
        * The output of the function has to be another individual object.
    The function applies this precoded functions to the dataframe and returns
    the output_dataframe.
    """
    if methodname == '':
        pass
    elif methodname == 'normalization':  # TODO: it is not implemented
        f = lambda text: text.decode('utf8')
        dataframe = dataframe.apply(f)
    elif methodname == 'str2num':
        # this method could create a column with some numbers as integers
        #and anothers as floats #TOTEST:  is this possible in pd df
        try:
            dataframe = dataframe.apply(int)
        except:
            #add another try for the error if it is not possible
            dataframe = dataframe.apply(float)
    return dataframe


def codification_precoded_functions(methodname, dataframe, dict4columns={}):
    """This is a function of functions. The conditions of these functions are:
        * The input of the function has to be a dataframe.
        * The output of the function has to be a dictionary of codes.
        or dicts of dicts
    """
    # TODO: Remember that it is a single operation. We have to do a loop.
    columns = list(dataframe.columns)
    dict4values = {}

    ## Initialization of the dict4columns.
    # For the variables that are not in the dataframe there are eliminated from
    # the dict.
    # The ones which are in the dataframe are initializated as {}.
    absentvalues = [e for e in columns if e not in dict4columns.keys()]
    for e in absentvalues:
        dict4columns[e] = {}
    # TODO: it should work with this part uncommented
#   intersectvalues = [ e for e in columns if e in dict4columns.keys()]
#   dict4values = dict((k, dict4columns[k]) for k in intersectvalues)
    dict4values = dict4columns

    for i in range(len(columns)):
        variable = columns[i]
        #EACH METHOD HAS TO RETURN A CODINGDICT
        if methodname == 'str2int':
            codingdict = dict4values[variable]
            keys = dataframe[variable].unique()
            for e in keys:
                try:
                    codingdict[e] = int(e)
                except:
                    codingdict[e] = 0     # value to be input maybe?
                    message = "WARNING: You have selected the codification "
                    message += "method 'str2int' but it fails when it tries "
                    message += "to pass the value: " + str(e) + "to an integer"
                    print(message)
        elif methodname == 'strbool2num':
            # todo: remove accents and normalize the strings in a correct
            #standart format.
            codingdict = dict4values[variable]
            keys = dataframe[variable].unique()

            lista_trues = ['true', 't', 'verdadero', 'si', 'positivo']
            lista_false = ['false', 'f', 'falso', 'no', 'negativo']

            for e in keys:
                e_str = str(e)
                try:
                    if e_str.lower() in lista_trues:
                        codingdict[e] = 1
                    elif e_str.lower() in lista_false:
                        codingdict[e] = 0
                    else:
                        codingdict[e] = np.nan
                except:
                    message = "WARNING: You have selected the codification "
                    message += "method 'strbool2num' but it fails when it "
                    message += "tries to pass the value: " + str(e)
                    message += "to an integer"
                    print(message)

        elif methodname == 'strbool2strnum':
            codingdict = dict4values[variable]
            keys = dataframe[variable].unique()

            lista_trues = ['true', 't', 'verdadero', 'si', 'positivo']
            lista_false = ['false', 'f', 'falso', 'no', 'negativo']

            for e in keys:
                e_str = str(e)
                try:
                    if e_str.lower() in lista_trues:
                        codingdict[e] = '1'
                    elif e_str.lower() in lista_false:
                        codingdict[e] = '0'
                    else:
                        codingdict[e] = ''
                except:
                    message = "WARNING: You have selected the codification "
                    message += "method 'strbool2strnum' but it fails when it "
                    message += "tries to pass the value: " + str(e)
                    message += "to an integer"
                    print(message)

        elif methodname == 'cat2num':
            codingdict = dict4values[variable]
            keys = dataframe[variable].unique()
            keys = [e for e in keys if e not in codingdict.keys()]
            for i in range(len(codingdict), len(codingdict) + len(keys)):
                codingdict[keys[i]] = i
        dict4columns[variable] = codingdict
    # TO Change!!!!!!!!!!!
    if len(columns) == 1:
        dict4columns = dict4columns[columns[0]]

    #print(dict4columns)
    return dict4columns


def dataframewise_precoded_function(methodname, dataframe):
    """This function contains precoded functions of transformation.
            * The input of the function has to be a dataframe.
            * The output of the function has to be a dataframe.
    """
    if methodname == 'select1of2':
        #this function receive a 2-column df and return 1 of them.
        col0 = dataframe.columns[0]
        col1 = dataframe.columns[1]
        return pd.DataFrame([dataframe.iloc[i][col1]
                            if not pd.isnull(dataframe.iloc[i][col1])
                            else dataframe.iloc[i][col0]
                            for i in range(dataframe.shape[0])])
    elif methodname == 'splitbyguion':
        #this function receive a 1-column df and return 2 created splitting.
        aux = dataframe[0].apply(lambda s: s.split('-'))
        return pd.DataFrame(list(aux))
    elif methodname == 'splitbydot':
        #this function receive a 1-column df and return 2 created splitting.
        aux = dataframe[0].apply(lambda s: s.split('.'))
        return pd.DataFrame(list(aux))


def normalize_string(data):
    """This function allows us to normalize the text to a same utf-8 unicode
    mode without accents."""
    try:
        data = data.decode('utf-8')
    except:
        pass
    # s = ''.join(x for x in unicodedata.normalize('NFKD', data)
    #             if unicodedata.category(x)[0] == 'L').lower()
    s = ''.join(x for x in unicodedata.normalize('NFKD', data)
                if unicodedata.category(x)[0] != 'Mn').lower()
    s = s.encode('utf-8')
    return s


def loading_standarization(data):
    """This function allows us to normalize the text to a same utf-8 unicode
    mode without accents."""
    try:
        data = str(data)
    except:
        pass
    try:
        data = data.decode('utf-8')
    except:
        pass

    # s = ''.join(x for x in unicodedata.normalize('NFKD', data)
    #             if unicodedata.category(x)[0] == 'L').lower()
    s = ''.join(x for x in unicodedata.normalize('NFKD', data)
                if unicodedata.category(x)[0] != 'Mn')
    s = s.encode('utf-8')
    return s
