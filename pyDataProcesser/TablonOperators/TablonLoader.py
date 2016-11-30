# -*- coding: utf-8 -*-
"""
TablonLoader
------------
Module to communicate with server-DB and upload data using url api.

"""

import os
import pandas as pd
from string import Template

#from TransformationFunction import *
#from DataDictObject import *
#from functions import *


def f_standart_url_creation(dataframe, columns, base_url):
    "Standart url creation."
    urls = []
    for i in range(dataframe.shape[0]):
        values = list(dataframe.iloc[i])
        variables_set = ["&"+str(columns[ii]) + "=" +
                         loading_standarization(values[ii])
                         for ii in range(len(columns))]
        urls.append(''.join(base_url+variables_set))
    return urls


class TablonLoaderDB:
    """The main aim of this class is describe the communication between this
    program in python with an external database.

    # QUESTIONS: What to do with self.urls??
    # TODO: Automatically get this info (or give it hardcoded).
    #   This variables are the ones that the LR API recognizes as inputs.
    # TODO: Run this script Automatically?
    # TODO: Set this ones to self.transformationfuncs.orderedvars
    # TODO: in write_urlfile ensure file existence.

    """
    processname = 'loader'

    def _initialization(self):
        ""
        # Memory
        self.urls = []
        self.memory = []

    def __init__(self, loading_script, baseurl='', loaderdict='', key='',
                 serverdb_vars=[], filepath='Outputs/loading_urls/',
                 f_url_creation=f_standart_url_creation):
        """This function initializes the datadictobject which has to lead the
        transformation to server-DB tablon-like with the variables of the
        server-DB API.
        It also have the inputs of the API and we have to respect these ones.

        """
        self._initialization()
        ## TRANSFORMATION
        # Transformation information
        if not loaderdict:
            self.transformationfuncs = DataDictObject('loader')
        else:
            self.transformationfuncs = loaderdict

        ## CREATION OF QUERIES
        # Building base of the url of the API to the server DB through url
        self.baseurl = baseurl
        self.key = key

        # Variables of the API of the server database.
        # Connection throuhg urls
        self.serverdb_vars = serverdb_vars

        ## USING EXTERNAL FILES TO OUTPUT RESULTS OR READ TEMPLATES
        # Path where there is the templates needed.
        pathfile = os.path.split(os.path.realpath(__file__))[0]
        templates_folder = '../../data/templates/database/'
        self.path_templates = os.path.join(pathfile, templates_folder)
        # File information of intern filetext
        self.filepath = os.path.join(pathfile, filepath)

        ## Set loading script file
        assert(os.path.isfile(loading_script))
        self.loading_script = loading_script

        ## Function to create the urls
        self.f_url_creation = f_url_creation

    ############################## Main functions #############################
    ###########################################################################
    def apply(self, input_):
        """Apply the main function of loading to the server database.
        """
        self.load(input_)

    def load(self, input_=[]):
        """This function is the one in charge to upload to the server the data.
        In order to do it, it has to receiceive the dataframe or use the data.
        All the correct urls used they have to be deleted from the self.urls
        list.

        The input could be the type:
            * no input: it will check for urls in the class
            * dataframe: this class will calculate the urls.
            * urls: these urls are the ones used to set the class parameter.

        """
        ## 1. Obtain the urls.
        if type(input) == list and not bool(input):
            if not self.urls:
                pass  # noo return or going outside the function
            else:
                urls = self.urls
        else:
            if type(input) == list:
                urls = input_
            elif type(input_) == pd.core.frame.DataFrame:
                urls = self.dataframe2DBqueries(input_)

        ## 2. Write the script
        self.write_urlfile(self.filepath, urls)

        ## 3. Run this script (problems to do it.
        # You have to do it in an external way or something like that)
        run_file(self.loading_script)

    ################################# Setters #################################
    ###########################################################################
    def set_functions_transformation(self, transformationfuncsdictobject):
        self.transformationfuncsdictobject = transformationfuncsdictobject

    def dataframe2DBqueries(self, dataframe, possiblevars=[]):
        """This function is able to transform a dataframe in a list
        of queries to the api of lead rating.
        We have to consider that the API only accepts some name variables
        and the others are ignored.

        """
        # 0. Initialization
        columns = dataframe.columns
        if possiblevars:
            self.serverdb_vars = possiblevars
        else:
            self.serverdb_vars = columns

        # 1. Control of the variables and its relation with the possible
        #variables of the API.
        # TODO: CHANGE THIS!!!!
        # In order to filter variables and ensure they enter in the urls.
        intersect_vars, lack_vars, absent_vars =\
            control_input(columns, self.serverdb_vars, (1, 0))
        dataframe = dataframe[intersect_vars]

        # 2. Creation of the urls
        base_url = [self.baseurl, self.key]
        urls = self.f_url_creation(dataframe, columns, base_url)

        self.urls = urls
        return urls

    def transform(self, dataframe, transformationfuncsdictobject=[]):
        """This function transform the data in the correct way we expect as we
        specify in the datadictobject related with loading.
        It only is used as a wrapper for the function 'transform_dataframe' of
        the 'DataDictObject'.
        """
        if not transformationfuncsdictobject:
            transformationfuncsdictobject = self.transformationfuncsdictobject
        dataframe =\
            transformationfuncsdictobject.transform_dataframe(dataframe)
        return dataframe

    def write_urlfile(self, filepath, urls):
        """This function has the goal of writing a script which process the
        loading to the database.
        """
        # Hardcoded !!
        fl = open(os.path.join(self.path_templates, 'parallel_urls.txt'), "r")
        filecode = fl.read()
        fl.close()

        new_urls = str(urls).replace(',', ",\n")
        filetext = Template(filecode).safe_substitute(urls=new_urls)

        open(os.path.join(filepath, 'parallel_urls.py'), 'w').close()
        f = open(os.path.join(filepath, 'parallel_urls.py'), 'r+')
        f.write(filetext)
