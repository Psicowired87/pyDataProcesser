
import numpy as np
import pandas as pd


class DescriptionVars:
    """Class for generating, storing and retrieving the individual description
    of each one of the variables of the dataset.
    There are different types of description fomats in this class:
        * Raw data: Plots, images and artificial description separated.
        * tex: export LaTeX format.

    """

    def __init__(self, variablename, vartype='', path_templates=''):
        """This function call the other ones and initialize all the values of
        this class in order to be stored and used for a superior class.
        If it is not input the dataframe it will wait for the
        calculate_description function calling.

        """
        # variable information.
        ## TODO: probably data-dict center should be used to generate
        #the initial information and the natural text.
        self.variablename = variablename
        self.vartype = vartype

        # Store plots, tables and text. RAW data.
        self.plots = {}
        self.tables = {}
        # Natural text and artificial generated one.
        # TODO: Natural have to be inserted in the initialization.
        self.text = {}

        # String in which is saved the whole page
        self.tex = ''

        # Measure of utility of this variabledescription_input
        # TODO: implement a function to calculate this.
        # It could be a list of values.
        self.utility_value = 0

        # We suppose it is the path of the templates
        self.path_templates = path_templates

    def calculate_description(self, dataframe, dataframe_y=''):
        """Calculate description is to fill the plots, tables required."""
        # NO return
        #TODO: Missing value replacement??
        #NaN and '' problems. Or it is better to do it in the data-dict center.
        column = dataframe.replace({self.variablename:
                                    {'': NaN}})[self.variablename]
        #column = dataframe[self.variablename]
        ############# GENERATE DESCRIPTION TABLE ################
        # calculation of the missing value proportion.
        m = column.size
        missing = (m - column.count())/m
        if self.vartype == '':
            #TODO: Add "intelligence" (identify str or numeric)
            self.vartype = 'Categorical'
        if self.vartype == 'Categorical':
            #TODO: calculate description
            categories = list(column.unique())
            number_cats = len(categories)
            if dataframe_y:
                conversion = []
                for e in categories:
                    # TOTEST
                    conversion.append(float(dataframe_y[column == e].mean()))
            # calculate Gini index or sth like or the unequality index
            mode = column.mode()[0]
            vmode = column[(column == mode)].count()

            table = pd.DataFrame([str(number_cats), mode, str(vmode),
                                  "{0:.2f}".format(missing*100) + ' %'])
            table = table.transpose()
            table.columns = ['# cats', 'mode', 'volumn_mode', '% missings']
        elif self.vartype == 'Ordinal':
            #TODO: Problems, could be ordinal but in string expression.
            # (ex: Bad, regular good.) Search for solutions.
            pass
        elif self.vartype == 'Numerical':
            #TODO: calculate description
            # column = lista[self.variablename].apply(int)
            # In theory it is formatted as we want.
            #Else we have a problem, but this line it shouldnt be needed.
            rang = [column.min(), column.max()]
            mean = column.mean()
            std = column.std()

            table = pd.DataFrame([str(rang), "{0:.2f}".format(mean),
                                  "{0:.2f}".format(std),
                                  "{0:.2f}".format(missing*100) + ' %'])
            table = table.transpose()
            table.columns = ['range', 'mean', 'std', '% missings']

            # probably histogram to calculate conversion?

        self.table['Description'] = table

        #########################################################
        #TODO: generate tables
        #TODO: generate plots

    def generate_latex_report(self, path_templates=''):
        """It generates a report of latex in which it is shown the whole
        description of the selected variable."""
        # TODO: read this texts from files in the package.

        #TODO:
        fl = open(self.path_templates + 'page.txt', "r")
        page = fl.read()
        fl.close()

        vardescription_str = self.text['Natural']
#        tabledescriptor_str = 
#        plots_str = 
#        comments_str = 
        artificialcomments_str = ''
        page = Template(page).\
            safe_substitute(vardescription=vardescription_str,
                            tabledescriptor=tabledescriptor_str,
                            plots=plots_str, comments=comments_str,
                            artificialcomments=artificialcomments_str)
        return page

    def generate_table_tex(self, nametable, table, title_table,
                           caption_bool=True):
        """Transform a table to a tex code using dataframe option a tabular
        tex environment."""
        # TODO: Read from a file.

        fl = open(self.path_templates + 'table.txt', "r")
        table_str = fl.read()
        fl.close()

        ## TODO:
        #       *types of tables
        #       *deal with strange names of variables or spaces
#        if table_title == :
#            description_caption = 
#        elif table_title == :
#            description_caption = 

        tablelabel_str = title_table + r'''_univariate''' + self.variablename

        if caption_bool:
            caption_str = Template(r'''\caption{$description}''').\
                safe_substitute(description=description_caption)
        else:
            caption_str = ''
        table = Template(table_str).\
            safe_substitute(tabular=table.to_latex(), caption=caption_str,
                            tablelabel=tablelabel_str)
        return table

    def generate_plots_tex(self, images, title_block_images,
                           caption_bool=True):
        """Generate the tex code to present images in the report."""
        if not images:
            #report error
            message = "This function needs to be passed the images. "
            message += "The variable " + self.variablename
            message += " needs to have an image."
            raise Exception(message)
        # figure environment generation

        fl = open(self.path_templates + 'image.txt', "r")
        image_str = fl.read()
        fl.close()

#        if title_block_images == :
#            description_caption = 
#        elif title_block_images == :
#            description_caption = 

        imagelabel_str =\
            title_block_images + r'''_univariate''' + self.variablename

        # figures tex generation and saving the files.
        l = len(images)
        if l == 1:
            graphics_str = r'''
            \includegraphics[width=0.9\textwidth]{$folder$imagename.png}
            '''
            graphics_str = Template(graphics_str).\
                safe_substitute(folder='', imagename='')
            # TODO: Save the files

        elif l == 2:
            graphics_str = r'''
            \includegraphics[width=0.45\textwidth]{$folder$imagename1.png}
            \includegraphics[width=0.45\textwidth]{$folder$imagename2.png}
            '''
            graphics_str = Template(graphics_str).\
                safe_substitute(folder='', imagename1='', imagename2='')
            # TODO: Save the files
        elif l == 3:
            graphics_str = r'''
            \includegraphics[width=0.45\textwidth]{$folder$imagename1.png}
            \includegraphics[width=0.45\textwidth]{$folder$imagename2.png}
            \includegraphics[width=0.45\textwidth]{$folder$imagename3.png}
            '''
            graphics_str = Template(graphics_str).\
                safe_substitute(folder='', imagename1='', imagename2='',
                                imagename3='')
            # TODO: Save the files
        elif l == 4:
            graphics_str = r'''
            \includegraphics[width=0.45\textwidth]{$folder$imagename1.png}
            \includegraphics[width=0.45\textwidth]{$folder$imagename2.png}
            \includegraphics[width=0.45\textwidth]{$folder$imagename3.png}
            \includegraphics[width=0.45\textwidth]{$folder$imagename4.png}
            '''
            graphics_str =\
                Template(graphics_str).safe_substitute(folder='',
                                                       imagename1='',
                                                       imagename2='',
                                                       imagename3='',
                                                       imagename4='')
            # TODO: Save the files
        # caption generation
        if caption_bool:
            caption_str = Template(r'''\caption{$description}''').\
                safe_substitute(description=description_caption)
        else:
            caption_str = ''

        image = Template(image_str).safe_substitute(graphics=graphics_str,
                                                    caption=caption_str,
                                                    imagelabel=imagelabel_str)

    def show_terminal(self):
        """Interactive function in order to show the properties of a variable
        in the terminal and take decisions easier.
        """
        for e in self.tables:
            print(e)
        for e in self.plots:
            e.show()


class DescriptionRelations:
    """It is a class which contains the description of relationships between
    variables.
    """
    def __init__(self, ):
        self.plots = {}

    def generate_latex_report(self,):
        """It generates a report of latex in which it is show the whole
        description of the selected variables relationship.
        """
        # subsection (variables names)
        return page


class TablonDescription:
    """This class is a collection of DescriptionVars. It contains the
    information of the data and the information of each variable.
    """

    def __init__(self, dataframe):
        ## information from client. Probably this information should be
        #integrated in the data-dict center
        self.clientname = ''
        self.codename = ''
        # Natural desctription
        # description of each ones of the variables in text.
        self.variabledescription_input = {}
        # Artificial description generated.
        self.variabledescription_output = {}
        # Text of the whole description.
        # description of the whole dataset.
        # It is a text string with the description.
        self.tex = ''
        # variable list obtained from dataframe
        self.variablelist = dataframe.columns
        ## Initialization to fill
        # TODO:  If we want less variables??
        # {variablename: object(DescriptionVars)}
        self.univariate_dict = {}
        # TODO:
        # TODO: List of tuple of lists?
        #First as heuristic easy computable measure as corr? Dummy
        self.bivariate_list = []
        # WTF is this??
        # initialize empty object of data description
        self.total_description = 

    def explore(self):
        """Explore the whole dataset. Fill the variabledescription_input.
        """
        # obtain the self.univariate_list and the others
        # TODO: The others.
        # Univariate study
        # TODO: We need the data-dict center in order to know the variabletype.
        for variablename in self.variablelist:
            self.univariate_dict[variablename] =\
                DescriptionVars(variablename, '')

    def show_report(self, variablename):
        """It shows in terminal all the information available of the desired
        variable."""
        description = self.variabledescription_output[variablename]
        #TODO
        description.show_terminal()
        return

    def export_results(self, filename='', pathfile='', documenttype=''):
        """This function export the results obtained in the description of the
        whole dataset. It compile the .*tex and obtain the pdf.

        References
        ----------
        [1] .. http://stackoverflow.com/questions/8085520/generating-pdf-latex-
        with-python-script

        """

        # Built a pdf as a concatenation of general instructions.
        if not pathfile:
            pathfile = "tex_templates/"
        ##### It is better to be read from a file.
        # header it is the usepackage part
        fl = open(pathfile + 'header.txt', "r")
        header = fl.read()
        fl.close()

        fl = open(pathfile + 'portada.txt', "r")
        portada = fl.read()
        fl.close()
        portada = Template(portada).safe_substitute(self.clientname)

        # from external file. It could include not only \tableofcontents
        fl = open(pathfile + 'indice.txt', "r")
        indice = fl.read()
        fl.close()
        ###########################################
        self.univariate_dict = self.generate_univariate(pathfile)
        # TODO: Include bivariate, conclusions and others ideas.
        content = header + portada + indice
        content += self.univariate_dict + '\n\n\n\end{document}'
        # Decode for useful recognition of the string in the latex format.
        content = content.decode('utf-8')
        #Use Texcaller to compile *.tex
        #import texcaller
        #texcaller.convert(source, source_format, result_format, max_runs)
        # returns a pair (result, info)
        # pdf, info = texcaller.convert(latex, 'LaTeX', 'PDF', 5)
        return

    def generate_univariate(self, pathfile):
        ''' Generate the self.univariate_list.'''
        # section.txt: It should contain $contents and $list_vars_description
        fl = open(pathfile + 'section.txt', "r")
        section = fl.read()
        fl.close()

        # probably we will need more than one type of spaces.
        fl = open(pathfile + 'space.txt', "r")
        space = fl.read()
        fl.close()

        contents_str = r''' '''
        for e in self.univariate_list:
            contents_str = contents_str + e.tex + space
        section = Template(section).safe_substitute(list_vars_description='',
                                                    contents=contents_str)
        return section

    def generate_bivariate(self):
        """Generate the self.bivariate_list."""
        return
