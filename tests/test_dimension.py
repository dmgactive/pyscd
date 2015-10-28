# -*- coding: utf-8 -*-

import unittest
import os
import pandas as pd
import tables as tb
from pyscd.dimension import SlowlyChangingDimension as scd


def import_orders(outfilename, infilename):
    df = pd.read_csv(infilename)
    df['order'] = df['order'].astype(str)

    store = pd.HDFStore(outfilename, 'a')
    store.append('orders', df, data_columns=True, index=False)
    store.close()


def import_workcenters(outfilename, workbook, worksheet):
    df = pd.read_excel(workbook, sheetname=worksheet, encoding='latin-1')
    df.columns = ['workcenter', 'description', 'group', 'hours']

    store = pd.HDFStore(outfilename, 'a')
    store.append('workcenters', df, data_columns=True, index=False)
    store.close()


class TestDimension(unittest.TestCase):
    def setUp(self):
        self.filename = '{!s}.h5'.format(__name__)

        self.store = None

        if os.path.isfile(self.filename):
            os.remove(self.filename)

    def tearDown(self):
        if self.store:
            self.store.close()

        if os.path.isfile(self.filename):
            os.remove(self.filename)

    def test_import_row_for_the_first_time(self):
        import_orders(self.filename, 'tests/data/orders x1.csv')

        self.store = pd.HDFStore(self.filename, mode='a',
            complevel=9, complib='blosc')

        dim = scd(store=self.store,
                  name='dimorders',
                  lookupatts=['order'],
                  type1atts=[],
                  type2atts=['line', 'status', 'currency'],
                  asof='2015-10-23')

        for chunk in pd.read_hdf(self.filename, 'orders', chunksize=1000):
            print(chunk.values.ravel())
            dim.update(chunk)
        del chunk

        result = self.store['dimorders']
        self.assertEqual(len(result), 1)
        self.assertEqual(result['order'][0], '1')
        self.assertEqual(result['line'][0], 10)
        self.assertEqual(result['status'][0], 'Not Delivered')
        self.assertEqual(result['currency'][0], 'USD')
        self.assertEqual(result['scd_id'][0], 1)
        self.assertEqual(result['scd_valid_from'][0], 1445558400000000000)
        self.assertEqual(result['scd_valid_to'][0], 7258032000000000000)
        self.assertEqual(result['scd_version'][0], 1)
        self.assertEqual(result['scd_current'][0], True)
        self.assertEqual(result['scd_hash'][0], '5357057d66f2ef4886169ace62e9892a6c479575')

    def test_import_same_row_again_does_not_duplicate(self):
        import_orders(self.filename, 'tests/data/orders x1.csv')

        self.store = pd.HDFStore(self.filename, mode='a',
            complevel=9, complib='blosc')

        dim = scd(store=self.store,
                  name='dimorders',
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for chunk in pd.read_hdf(self.filename, 'orders', chunksize=1000):
            dim.update(chunk)
        del chunk

        dim = scd(store=self.store,
                  name='dimorders',
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for chunk in pd.read_hdf(self.filename, 'orders', chunksize=1000):
            dim.update(chunk)
        del chunk

        result = self.store['dimorders']
        self.assertEqual(len(result), 1)

    def test_add_new_row(self):
        # Update dimension with first file
        import_orders(self.filename, 'tests/data/orders x1.csv')

        self.store = pd.HDFStore(self.filename, mode='a',
            complevel=9, complib='blosc')

        dim = scd(store=self.store,
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

        dim = scd(store=self.store,
                  name='dimorders',
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for chunk in pd.read_hdf(self.filename, 'orders', chunksize=1000):
            dim.update(chunk)
        del chunk

        result = self.store['dimorders']
        self.assertEqual(len(result), 2)

        result = result.loc[result['line'] == 20]
        self.assertEqual(result['order'][0], '1')
        self.assertEqual(result['line'][0], 20)
        self.assertEqual(result['status'][0], 'Completed')
        self.assertEqual(result['currency'][0], 'USD')
        self.assertEqual(result['scd_id'][0], 2)
        self.assertEqual(result['scd_valid_from'][0], 1445558400000000000)
        self.assertEqual(result['scd_valid_to'][0], 7258032000000000000)
        self.assertEqual(result['scd_version'][0], 1)
        self.assertEqual(result['scd_current'][0], True)
        self.assertEqual(result['scd_hash'][0], '4297cd7da3357b8fdd754c47a39ffae562fb7cd4')

    def test_modify_type_1_column(self):
        # Update dimension with first file
        import_orders(self.filename, 'tests/data/add 1 row.csv')

        self.store = pd.HDFStore(self.filename, mode='a',
            complevel=9, complib='blosc')

        dim = scd(store=self.store,
                  name='dimorders',
                  lookupatts=['order', 'line'],
                  type1atts=['status'],
                  type2atts=['currency'],
                  asof='2015-10-23')

        for chunk in pd.read_hdf(self.filename, 'orders', chunksize=1000):
            dim.update(chunk)
        del chunk

        # Update dimension with second file
        import_orders(self.filename, 'tests/data/modify 1 row.csv')

        self.store = pd.HDFStore(self.filename, mode='a',
            complevel=9, complib='blosc')

        dim = scd(store=self.store,
                  name='dimorders',
                  lookupatts=['order', 'line'],
                  type1atts=['status'],
                  type2atts=['currency'],
                  asof='2015-10-23')

        for chunk in pd.read_hdf(self.filename, 'orders', chunksize=1000):
            dim.update(chunk)
        del chunk

        result = self.store['dimorders']
        self.assertEqual(len(result), 2)

        print(result)

        result = result.loc[result['order'] == 1]
        self.assertEqual(result['order'][0], '1')
        self.assertEqual(result['line'][0], 10)
        self.assertEqual(result['status'][0], 'Completed')
        self.assertEqual(result['currency'][0], 'USD')
        self.assertEqual(result['scd_id'][0], 1)
        self.assertEqual(result['scd_valid_from'][0], 1445558400000000000)
        self.assertEqual(result['scd_valid_to'][0], 7258032000000000000)
        self.assertEqual(result['scd_version'][0], 1)
        self.assertEqual(result['scd_current'][0], True)
        self.assertEqual(result['scd_hash'][0], 'bb55c40c01107eb9d3e2a9aa706dc2ee8217343e')
