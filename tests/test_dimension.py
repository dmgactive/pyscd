# -*- coding: utf-8 -*-

import unittest
import os
import pandas as pd
import tables as tb
from pyscd.dimension import SlowlyChangingDimension as scd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def import_orders(outfilename, infilename):
    df = pd.read_csv(infilename)
    df['order'] = df['order'].astype(str)

    store = pd.HDFStore(outfilename, 'a')
    store.append('orders', df, data_columns=True, index=False, append=False)
    store.close()


def import_workcenters(outfilename, workbook, worksheet):
    df = pd.read_excel(workbook, sheetname=worksheet, encoding='latin-1')
    df.columns = ['workcenter', 'description', 'group', 'hours']

    store = pd.HDFStore(outfilename, 'a')
    store.append('workcenters', df, data_columns=True, index=False)
    store.close()


class TestDimension(unittest.TestCase):
    def setUp(self):
        self.filename = 'test.h5'

        if os.path.isfile(self.filename):
            os.remove(self.filename)

    def tearDown(self):
        self.store.close()

        if os.path.isfile(self.filename):
            os.remove(self.filename)

    def test_import_row_for_the_first_time(self):
        import_orders(self.filename, 'tests/data/orders x1.csv')

        dim = scd(path=self.filename,
                  name='dimorders',
                  lookupatts=['order'],
                  type1atts=[],
                  type2atts=['line', 'status', 'currency'],
                  asof='2015-10-23')

        for chunk in pd.read_hdf(self.filename, 'orders', chunksize=1000):
            print(chunk.values.ravel())
            dim.update(chunk)
        del chunk

        self.store = pd.HDFStore(self.filename, mode='a',
            complevel=9, complib='zlib')

        result = self.store['dimorders']
        self.assertEqual(len(result), 1)
        self.assertEqual(result['order'].values[0], '1')
        self.assertEqual(result['line'].values[0], 10)
        self.assertEqual(result['status'].values[0], 'Not Delivered')
        self.assertEqual(result['currency'].values[0], 'USD')
        self.assertEqual(result['scd_id'].values[0], 1)
        self.assertEqual(result['scd_valid_from'].values[0], 1445558400000000000)
        self.assertEqual(result['scd_valid_to'].values[0], 7258032000000000000)
        self.assertEqual(result['scd_current'].values[0], True)
        self.assertEqual(result['scd_hash'].values[0], '39510ad9dc54f9e05bb3cf9db33ab1a1b0b66114')

    def test_import_same_row_again_does_not_duplicate(self):
        import_orders(self.filename, 'tests/data/orders x1.csv')

        dim = scd(path=self.filename,
                  name='dimorders',
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for chunk in pd.read_hdf(self.filename, 'orders', chunksize=1000):
            dim.update(chunk)
        del chunk

        dim = scd(path=self.filename,
                  name='dimorders',
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for chunk in pd.read_hdf(self.filename, 'orders', chunksize=1000):
            dim.update(chunk)
        del chunk

        self.store = pd.HDFStore(self.filename, mode='a',
            complevel=9, complib='zlib')

        result = self.store['dimorders']
        self.assertEqual(len(result), 1)

    def test_add_new_row(self):
        # Update dimension with first file
        import_orders(self.filename, 'tests/data/orders x1.csv')

        dim = scd(path=self.filename,
                  name='dimorders',
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for chunk in pd.read_hdf(self.filename, 'orders', chunksize=1000):
            dim.update(chunk)
        del chunk

        # Update dimension with second file
        import_orders(self.filename, 'tests/data/add 1 row.csv')

        dim = scd(path=self.filename,
                  name='dimorders',
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for chunk in pd.read_hdf(self.filename, 'orders', chunksize=1000):
            dim.update(chunk)
        del chunk

        self.store = pd.HDFStore(self.filename, mode='a',
            complevel=9, complib='zlib')

        result = self.store['dimorders']
        self.assertEqual(len(result), 2)

        self.assertEqual(result['order'].values[1], '1')
        self.assertEqual(result['line'].values[1], 20)
        self.assertEqual(result['status'].values[1], 'Completed')
        self.assertEqual(result['currency'].values[1], 'USD')
        self.assertEqual(result['scd_id'].values[1], 2)
        self.assertEqual(result['scd_valid_from'].values[1], 1445558400000000000)
        self.assertEqual(result['scd_valid_to'].values[1], 7258032000000000000)
        self.assertEqual(result['scd_current'].values[1], True)
        self.assertEqual(result['scd_hash'].values[1], '47580ba821ac3f942c13582f88a73c644241396a')

    def test_modify_type_1_column(self):
        # Update dimension with first file
        import_orders(self.filename, 'tests/data/add 1 row.csv')

        dim = scd(path=self.filename,
                  name='dimorders',
                  lookupatts=['order', 'line'],
                  type1atts=['status'],
                  type2atts=['currency'],
                  asof='2015-10-23')

        for chunk in pd.read_hdf(self.filename, 'orders', chunksize=1000):
            dim.update(chunk)
        del chunk
        del dim

        # Update dimension with second file
        import_orders(self.filename, 'tests/data/modify 1 row.csv')

        self.store = pd.HDFStore(self.filename, mode='a',
            complevel=9, complib='zlib')

        dim = scd(path=self.filename,
                  name='dimorders',
                  lookupatts=['order', 'line'],
                  type1atts=['status'],
                  type2atts=['currency'],
                  asof='2015-10-23')

        for chunk in pd.read_hdf(self.filename, 'orders', chunksize=1000):
            dim.update(chunk)
        del chunk
        del dim

        self.store = pd.HDFStore(self.filename, mode='a',
            complevel=9, complib='zlib')

        result = self.store['dimorders']
        print('Result Dataframe:\n', result)
        self.assertEqual(len(result), 2)

        self.assertFalse(result.empty)
        self.assertEqual(result['order'].values[0], '1')
        self.assertEqual(result['line'].values[0], 10)
        self.assertEqual(result['status'].values[0], 'Completed')
        self.assertEqual(result['currency'].values[0], 'USD')
        self.assertEqual(result['scd_id'].values[0], 1)
        self.assertEqual(result['scd_id'].values[1], 2)
        self.assertEqual(result['scd_valid_from'].values[0], 1445558400000000000)
        self.assertEqual(result['scd_valid_to'].values[0], 7258032000000000000)
        self.assertEqual(result['scd_current'].values[0], True)
        self.assertEqual(result['scd_hash'].values[0], '0d4f629999f2dd1a2b37059f7f5364564a51ad37')

    def test_modify_type_2_column(self):
        # Update dimension with first file
        import_orders(self.filename, 'tests/data/add 1 row.csv')

        dim = scd(path=self.filename,
                  name='dimorders',
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for chunk in pd.read_hdf(self.filename, 'orders', chunksize=1000):
            dim.update(chunk)
        del chunk
        del dim

        # Update dimension with second file
        import_orders(self.filename, 'tests/data/modify 1 row.csv')

        dim = scd(path=self.filename,
                  name='dimorders',
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for chunk in pd.read_hdf(self.filename, 'orders', chunksize=1000):
            dim.update(chunk)
        del chunk
        del dim

        self.store = pd.HDFStore(self.filename, mode='a',
            complevel=9, complib='zlib')

        result = self.store['dimorders']
        print('Result Dataframe:\n', result)
        self.assertEqual(len(result), 3)

        self.assertFalse(result.empty)
        self.assertEqual(result['order'].values[0], '1')
        self.assertEqual(result['line'].values[0], 10)
        self.assertEqual(result['status'].values[0], 'Not Delivered')
        self.assertEqual(result['status'].values[2], 'Completed')
        self.assertEqual(result['currency'].values[0], 'USD')
        self.assertEqual(result['scd_id'].values[0], 1)
        self.assertEqual(result['scd_id'].values[2], 3)
        self.assertEqual(result['scd_valid_from'].values[0], 1445558400000000000)
        self.assertEqual(result['scd_valid_from'].values[2], 1445558400000000000)
        self.assertEqual(result['scd_valid_to'].values[0], 1445558400000000000)
        self.assertEqual(result['scd_valid_to'].values[2], 7258032000000000000)
        self.assertEqual(result['scd_current'].values[0], False)
        self.assertEqual(result['scd_current'].values[2], True)
        self.assertEqual(result['scd_hash'].values[0], '39510ad9dc54f9e05bb3cf9db33ab1a1b0b66114')
        self.assertEqual(result['scd_hash'].values[2], '0d4f629999f2dd1a2b37059f7f5364564a51ad37')
