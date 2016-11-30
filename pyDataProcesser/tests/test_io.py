
"""


"""

import os
import pandas as pd

from ..IO import get_extension_file, parse_manual_csv, parse_xlsx,\
    parse_dataframe


def test():

    filename = 'csv_example.csv'
    get_extension_file(filename)

    pathtests = os.path.split(os.path.realpath(__file__))[0]
    filespath = os.path.join(pathtests, '../../data/test_data/')
    csvpath = os.path.join(filespath, 'csv_example.csv')
    xlsxpath = os.path.join(filespath, 'xlsx_example.xlsx')

    data = parse_manual_csv(csvpath, delimiter=';', header=True,
                            normalize_str=False)
    assert(type(data) == pd.DataFrame)

    data = parse_xlsx(xlsxpath, sheets=None, normalize_str=False)
    assert(type(data) == dict)
    parse_xlsx(xlsxpath, sheets=0, normalize_str=False)
    assert(type(data) == dict)


    #parse_dataframe(filepath, extension, name_data='', args=[])
