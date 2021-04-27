from unittest import TestCase
import re
import unittest
import pandas as pd
from typing import Dict


# reader csv class
# query reader class


def input_to_pandas_type(dtype):
    types = {
        'string': 'object',
    }
    return types.get(dtype, dtype)

def get_column_types(string) -> Dict:
    """

    """
    col_names = string.split(',')
    out_dict = {}
    for col in col_names:
        dname = col.split('[')[0].strip()
        dtype = col.split('[')[1].strip(']').strip()
        out_dict.update({dname: dtype})
    return out_dict


def readCsvFile(filepath: str, type_tranlator = lambda ctype: ctype) -> pd.DataFrame:
    # TODO type translator
    with open(filepath) as csv_file:
        header = csv_file.readline()
    dtypes = get_column_types(header)
    # apply type translator here
    # use map(..) instead
    for col in dtypes:
        dtypes[col] = type_tranlator(col)
    df = pd.read_csv(filepath, skiprows=[0], names=dtypes)
    return df



def read_query(string, df, types)
    """
    verifuy query valid and map to useful string.
    """
    string
    comparison = ['=', '<', '<=', '!=']
    getregex = lambda lis = re.compile(f"({'|'.join(lis)})")
    setops = ["&&", "||"]
    # split on and/or
        # split on operand
        # assert col exists 
        # asser datatype is valid
    lambda_list = []
    for match in re.split(getregex(setops), string):
        # looping over  a single set of criterion
        pair = re.split(getregex(comparison), match))
            # 5 < col < 10
        name = pair[0].strip()
        value = pair[1].strip()
        # check if column exists 
        if pair[0].strip() not in df.columns:
            # give reason
            return False
        #lambda pd_dtype: list(filter(lambda key: input_to_pandas_type.types.get(key) == pd_dtype, input_to_pandas_type.types.key))[0]
        dtype = types.get(name)
        if dtype == 'int':
            value = int(value)
        elif dtype == 'float':
            value = float(value)
        elif dtype = 'string':
            pass
        elif dtype = 'bool':
            value = False if value = 'false' else True
        else:
            # reason: unknow column
            return False
        if input_to_pandas_type(dtype) != str(df[name].dtype):
            # reason: wrong value type
            return False

        # extract the comparison at this point
        # assuming only ever return what we want... lazy
        def criterion(row):
            # assuming dtype, name, value, comparator are copied by value
            comparator = list(filter(lambda chunk: chunk in comparison, match.split(' ')))[0]
            if comparator == '<':
                row.get(name) < value
            elif comparator == '>':
                row.get(name) < value
            elif comparator == '=':
                row.get(name) < value
            elif comparator == '!=':
                row.get(name) < value
            elif comparator == '<=':
                row.get(name) < value
            elif comparator == '>=':
                row.get(name) < value

        # map pd -> pyrthon natives

    """
    list of sets of criterion
        criterion are combined dependnent on setops

    """



class funcTestCases(TestCase):
    @unittest.skip('')
    def test_get_column_types_smoke(self):

        col_string = 'order_id[int], stock[string], price[float], size[int], executed[bool]'
        expected = {
            'order_id': 'int',
            'stock': 'string',
            'price': 'float',
            'size': 'int',
            'executed': 'bool'
        }
        res = get_column_types(col_string)
        self.assertDictEqual(res, expected)

    @unittest.skip('')
    def test_readCsvFile(self):
        filepath = '/home/rory/dev/py_sandbox/test/test_data.csv'
        df = readCsvFile(filepath)
        self.assertListEqual(list(df.columns), ['order_id', 'stock', 'price', 'size', 'executed'])
        self.assertTrue(isinstance(df, pd.DataFrame))
        self.assertListEqual(list(df.dtypes, ['int', 'object', 'float', 'int', 'bool']))

    def test_read_query(self):
        filepath = '/home/rory/dev/py_sandbox/test/test_data.csv'
        q = 'order_id = 3'
        df = readCsvFile(filepath)
        q = read_query(q, df)
        self.assertTrue(q)
        pass

if __name__ == '__main__':
    unittest.main(verbosity=2)



