# -*- coding: utf-8 -*-

import pandas as pd
from datetime import datetime
import numpy as np

from DataDictObject import DataDictObject
from TablonReader import TablonReader
from TablonEncoder import TablonEncoder
from TablonLoader import TablonLoaderDB


class DataManagementObject:
    """This is the class which contains the whole parser pipeline in order to
    transform between the different possible states of the data.
    These states are:
        * RawData: this format is identified by a string which is the path
        and the filename of the raw data.
        * Tablon: this format is the data parsed and each column formatted
        to one of the possible accepted formats of a common DB.
        It is characterized by a pandas dataframe.
        * DataFrame: this format contains special types of objects which can
        describe better the essence of the data and allow to be analize in
        special ways. It is characterized by a pandas dataframe.
        * DB: this is the server database. It is characterized by the
        direction of the database.
    """

    def __init__(self, client='', client_code='', pathdata='',
                 date_compilationdata='', delimiter='', typefile=''):
        """This object is recommended to be initialized which as information as
        we can give to it.
        """
        # common for the client. It is supposed not to change
        self.client = client
        self.client_code = client_code

        # for the same client we can use different files. This could change.
        self.pathdata = pathdata
        self.date_compilationdata = date_compilationdata
        self.date_treatmentdata = datetime.now()
        self.delimiter = delimiter

        # Initialization of the central object of the data process.
        #It will save all the information.
        self.datatransformation = DataProcessCenter()

    # The script only requires this one.
    def add_datadictobject(self, dataprocessobject):
        self.datatransformation = dataprocessobject

    # Collection of functions to set the class DataProcessCenter at this level.
    def add_parser(self, parser):
        self.datatransformation.parser = parser

    def add_encoder(self, encoder):
        self.datatransformation.encoder = encoder

    def add_format(self, format):
        self.datatransformation.format = format

    def add_loader(self, loader):
        self.datatransformation.loader = loader

    def add_preexploratory(self, preexploratory):
        self.datatransformation.preexploratory = preexploratory

    # Collection of functions for specify information of the
    def add_parser_parserdict(self, parserdict):
        self.datatransformation.parser.add_transformationdict(parserdict)

    def add_parser_expansiondict(self, expansiondict):
        self.datatransformation.parser.add_expansiondict(expansiondict)

    def add_parser_filteringdict(self, filteringdict):
        self.datatransformation.parser.add_filteringdict(filteringdict)

    def add_encode_encoderdict(self, encoderdict):
        self.datatransformation.encoder.add_transformationdict(encoderdict)

    def add_encode_expansiondict(self, expansiondict):
        self.datatransformation.encoder.add_expansiondict(expansiondict)

    def add_encode_filteringdict(self, filteringdict):
        self.datatransformation.encoder.add_filteringdict(filteringdict)

    def add_format_formatdict(self, formatdict):
        self.datatransformation.format.add_transformationdict(formatdict)

    def add_format_expansiondict(self, expansiondict):
        self.datatransformation.format.add_expansiondict(expansiondict)

    def add_format_filtering(self, filteringdict):
        self.datatransformation.format.add_filteringdict(filteringdict)

    def add_loader_loaderdict(self, loaderdict):
        self.datatransformation.loader.add_transformationdict(loaderdict)

    def add_loader_expansiondict(self, expansiondict):
        self.datatransformation.loader.add_expansiondict(expansiondict)

    def add_loader_filtering(self, filteringdict):
        self.datatransformation.loader.add_filteringdict(filteringdict)

    # calling to self.datatransformation
    # TODO: it has to be implemented the inputs of the data.
    def parse(self, filepath):
        self.datatransformation.parse(filepath)

#   def encode(self,tablon):
#   def format(self,tablon):
#   def load(self,tablon):

    def apply_pipeline(self, pipelineselection, startdataframe=''):
        """Run all the process of the data for the determine processes in the
        pipelineselection list or numpy array ({0,1} or boolean).
        """
        self.datatransformation.apply_pipeline(pipelineselection,
                                               startdataframe)

    # TODO: probably has to be done outside
    ################
    def save_Manager(self, filename_manager, path_manager=''):
        """With this function we are able to save the manager and all the
        information stored in order to redo the process of the data.
        """
        pass

    def load_Manager(self, filename_manager, path_manager=''):
        """With this function we are able to load the manager and all the
        information stored in order to redo the process with other data.
        It returns an object which we can change the main parameters but we
        keep the parser, format, codification and loading configuration.
        """
        pass

    #pipeline?
    def show(self):
        """TODO:"""
        return


class DataProcessCenter:
    """This class is oriented to agglomerate all the processes related with
    the dictionaries and settings of the data treatment.
    It works as an interface between the general pipeline class
    (DataManagementObject) and the class related to the basic treatment
    information of the data (DataDictObject).
    """
    # TODO: generalize self.input, self.process, self.output
    # TODO: exploratory and preexploratory in TablonDescriptor

    # TODO: probablente mejor un dictionary como input
    def __init__(self, client):
        """The initialization of the DataDictCente. It initializes all the the
        subprocesses.
        """
        # Read parameter file.
        ## That file has to retrieve:
        # key
        # filename
        # path_data
        # baseurl
        execfile("Scripts/" + client + "_parameters.py")
        compulsoryvars = {'filename': '', 'path_data': 'Data/', 'key': '',
                          'baseurl': ''}
        for e in compulsoryvars:
            if not e in locals():
                exec("global " + e)
                exec(e + ' = ' + '"' + compulsoryvars[e] + '"')
        self.filename = filename
        self.path_data = path_data

        # Initialize operators
        self.parser = TablonReader(DataDictObject('parser'))
        self.encoder = TablonEncoder(DataDictObject('encoder'))
        self.loader = TablonLoaderDB(DataDictObject('loader'), key, baseurl)
        self.preexploratory = DataDictObject('preexploratory')
        self.exploratory = DataDictObject('exploratory')

        # Initialize pipeline
        self.pipeline = [self.parser, self.preexploratory, self.encoder,
                         self.exploratory, self.loader]
        self.pipelineselection = ['parser', 'preexploratory', 'encoder',
                                  'loader', 'exploratory']

        # The processes that could generate an input to this system
        self.inputters = ['parser', 'downloader']

        # The processes that generate a tablon
        self.tabloners = ['encoder'] + self.inputters

        # The other parematers
        self.typeoutput = ''  # tablon, formattablon,analyticaltablon, others
        self.defaultpipe = ['parser', 'preexploratory', 'encoder', 'loader',
                            'exploratory']

    def apply_pipeline(self, pipelineselection, startdataframe=''):
        """This function apply a selection of tasks indicated in the numpy
        {0,1} or logical vector, or list of tasks indicated in order.
        The list of tasks could produce error because of the selection of an
        incorrect order.
            The possible tasks we could select are:
                * parser: only requires the file with structured data in order
                to
                * format: only requires the tablon data.
                This is obtained if there is a parser before that.
                * encoder: only requires the tablon data.
                This is obtained if there is a parser before that.
                * loader: only requires the tablon data.
                This is obtained if there is a parser before that.
                * preexploratory: only requires the tablon data.
                This is obtained if there is a parser before that.
                * exploratory: requires a parser and a encoder before this
                function.

        """

        ## 1. Creation of the pipelineselection
        ''' Reglas:
                * First an inputter.
                * Only of each type.
        '''
        # most general form of representation of the pipeline is the list,
        # because allow to the user to define an order.
        # We will transform the other ways of expressing the pipeline to this
        #one.

        # Initial setting
        default_pipeline = ['encoder', 'loader', 'exploratory']
        boolstartdf = True
        if not startdataframe:
            boolstartdf = False
            default_pipeline = ['parser', 'encoder', 'loader', 'exploratory']

        # Check and transform to list
        if type(pipelineselection) == list:
            # Only the first as inputter or any depend on the input.
            if boolstartdf:
                ind_inputters_incorrect =\
                    [i for i, x in enumerate(pipelineselection)
                     if x in self.inputters]
            else:
                ind_inputters_incorrect =\
                    [i for i, x in enumerate(pipelineselection)
                     if x in self.inputters and i != 0]

            pipelineselection = list(np.delete(pipelineselection,
                                               ind_inputters_incorrect))

            # Delete the repeated elements:
            pipelineorder = []
            for e in pipelineselection:
                if e not in pipelineorder:
                    pipelineorder.append(e)
                #occurrencies = [operation for operation in pipelineselection
                #                if operation == e]
                #if len(occurrencies) > 1:

            #TODO: Not only one of each type.
            #TODO: Delete consecutive encoders
            #TODO: Delete consecutive descriptions:
            pipelineselection = pipelineorder

        else:
            message = "WARNING: The pipeline given it is incorrect. "
            message += "It will be done all the tasks in the default order"
            print(message)
            pipelineselection = default_pipeline
            # Now we have a list with processes in order.

        self.pipelineselection = pipelineselection

        ## 2. Apply in this order.
        if boolstartdf:
            output = startdataframe
        else:
            output = []

        for operatoname in pipelineselection:
            if operatoname in self.tabloners:
                output = self.apply_operator(operatoname, output)
            else:
                self.apply_operator(operatoname, output)
        return output

    ################################ Processes ################################
    ###########################################################################
    def apply_operator(self, operatorname, dataframe):
        # TOFINISH
        if operatorname == 'parser':
            # check if parser the object
            if type(dataframe) == tuple:
                dataframe = dataframe[0]
            return self.parser.parse(self.filename, self.path_data)
        elif operatorname == 'format':
            # check if format the object
            if type(dataframe) == tuple:
                dataframe = dataframe[0]
            return self.format.format()
        elif operatorname == 'encoder':
            # check if encoder the object
            if type(dataframe) == tuple:
                dataframe = dataframe[0]
            return self.encoder.encode(dataframe)
        elif operatorname == 'loader':
            # check if loader the object
            if type(dataframe) == tuple:
                dataframe = pd.concat(dataframe, axis=1)
            self.loader.load(dataframe)
            return
        #TODO:
        elif operatorname == 'preexploratory':
            # check if preexploratory the object
            return
        elif operatorname == 'exploratory':
            # check if exploratory the object
            return
        elif operatorname == 'general':
            # check if exploratory the object
            # raise Exception
            pass
        else:
            # raise Exception
            pass

    def parse(self, filename, pathfile):
        return self.parser.parse(filename, pathfile)

    def format(self):
        return self.format.format()

    def encode(self):
        return self.encoder.encode()

    def load(self, input_):
        return self.loader.load(input_)

    def preexplore(self):
        return self.preexploratory.explore()

    def explore(self):
        return self.exploratory.explore()

    ############################# Setting objects #############################
    ###########################################################################
    def set_TablonReader(self, TablonReader):
        self.parser = TablonReader

    def set_TablonFormater(self, TablonFormater):
        self.format = TablonFormater

    def set_TablonEncoder(self, TablonEncoder):
        self.encoder = TablonEncoder

    def set_TablonLoader(self, TablonLoader):
        self.loader = TablonLoader

    def set_TablonMakers(self, tablondicts):
        for e in tablondicts:
            if e == 'TablonReader' or e == 'parser':
                self.parser = tablondicts[e]
            elif e == 'TablonFormater' or e == 'formater' or e == 'format':
                self.format = tablondicts[e]
            elif e == 'TablonEncoder' or e == 'encoder':
                self.encoder = tablondicts[e]
            elif e == 'TablonLoader' or e == 'loader':
                self.loader = tablondicts[e]
            else:
                message = "The only options of names to set are: TablonReader,"
                message += " parser, TablonFormater, formater, format, "
                message += "TablonEncoder, encoder, TablonLoader or loader"
                print(message)

    ################################# TOFINISH ################################
    ###########################################################################
    def set_datadictobject(self, datadictobject):
        # TOFINISH
        filename, pathfile = self.filename, self.pathfile
        if datadictobject.processtype == 'parser':
            # check if parser the object
            self.parser = TablonReader(filename, pathfile, object)

        elif datadictobject.processtype == 'format':
            # check if format the object
            self.format = TablonFormater(filename, pathfile, object)

        elif datadictobject.processtype == 'encoder':
            # check if encoder the object
            self.encoder = TablonEncoder(filename, pathfile, object)

        elif datadictobject.processtype == 'loader':
            # check if loader the object
            self.loader = TablonLoaderDB(filename, pathfile, object)

        #TODO:
        elif name == 'preexploratory':
            # check if preexploratory the object
            pass

        elif name == 'exploratory':
            # check if exploratory the object
            pass

        elif name == 'general':
            # check if exploratory the object
            # raise Exception
            pass

        else:
            # raise Exception
            pass
