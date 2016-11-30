
"""
TablonDownloader
----------------
The module for the interaction of the downloader.


# HELP:
http://pandas.pydata.org/
pandas-docs/stable/generated/pandas.read_sql_table.html
http://docs.sqlalchemy.org/en/rel_0_9/orm/tutorial.html#connecting

"""

import pandas as pd
import MySQLdb
#import sqlalchemy


class TablonDownloader:
    """This is a class specified in the task of downloading an specific table
    to your manager program of data in python.
    Using inputs of the direction of the table in the database and the
    database, this class performs a transformation and download the data into a
    pandas dataframe in the equivalent way as TablonReader is able to parse a
    file to a pandas dataframe.

    """
    processname = 'downloader'

    def _initialization(self):
        # Save information of the query and db
        self.memory = []

    def __init__(self, user, passwd, DBAdress, DBName, mainTable):
        "Main information to communicate with the server database."
        ## Main server-DB information
        DBAdress = '10.3.14.33'
        DBName = 'lr'
        host = 'mysql://' + DBAdress + '@localhost/' + DBName
        ## Connection pointer instantiation
        con = MySQLdb.connect(host=host,           # your host
                              user=user,           # your username
                              passwd=passwd,       # your password
                              db=DBName)           # name of the data base.
        self.con = con
        ## Table we want to inspect and retrieve inforation from
        self.mainTable = mainTable

    def DB_download(self, client_code, list_date_vars):
        """The main purpose of this function is performs a download of a table
        from a given database.

        """
        ## Create the connection to the DB
        sql = "SELECT * FROM " + self.mainTable
        sql += " WHERE account_id = " + str(client_code)

        ## Download Table from the DB
        dataframe = pd.read_sql(sql, self.con, parse_dates=list_date_vars)
        return dataframe
