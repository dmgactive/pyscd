# -*- coding: utf-8 -*-

import unittest
import os
import pandas as pd
import tables as tb
from pyscd.dimension import SlowlyChangingDimension as scd


class Dimension(tb.IsDescription):
    order           = tb.StringCol(255, pos=1)
    line            = tb.Int64Col(pos=2)
    status          = tb.StringCol(255, pos=3)
    currency        = tb.StringCol(255, pos=4)
    scd_id          = tb.Int64Col(pos=5)
    scd_valid_from  = tb.Int64Col(pos=6)
    scd_valid_to    = tb.Int64Col(pos=7)
    scd_version     = tb.Int16Col(pos=8)
    scd_current     = tb.BoolCol(pos=9)


def create_dimension(h5file):
    filters = tb.Filters(complevel=9, complib='zlib')
    group = h5file.create_group("/", 'dimorders')
    table = h5file.create_table(group, 'table', Dimension, filters=filters)
    table.cols.order.create_index()


def into(outfilename, infilename):
    df = pd.read_csv(infilename)
    df['order'] = df['order'].astype(str)

    store = pd.HDFStore(outfilename, 'a')
    store.append('orders', df, data_columns=True, index=False)
    store.close()


class TestDimension(unittest.TestCase):
    def setUp(self):
        self.filename = 'test.h5'

        if os.path.isfile(self.filename):
            os.remove(self.filename)

        self.h5file = tb.open_file(self.filename, mode='a')
        create_dimension(self.h5file)
        self.h5file.close()

    def tearDown(self):
        self.h5file.close()
        # if os.path.isfile(self.filename):
            # os.remove(self.filename)

    def test_import_new_data_fill_scd_columns(self):
        self.h5table = self.h5file.root.orders.table
        self.h5dim = self.h5file.root.dimorders.table

        dim = scd(connection=self.h5dim,
                  lookupatts=['order'],
                  type1atts=[],
                  type2atts=['line', 'status', 'currency'],
                  asof='2015-10-23')

        for row in self.h5table.iterrows():
            dim.update(row)
        self.h5dim.flush()

        expected = str((b'1', 10, b'Not Delivered', b'USD',
                        0, 1445558400000000000, 7258032000000000000, 1, True))

        self.assertEqual(len(self.h5dim), 1)
        self.assertEqual(str(self.h5dim[0]), expected)

    # def test_import_same_data_does_not_duplicate_row(self):
    #     dim = scd(connection=self.h5dim,
    #               lookupatts=['order', 'line'],
    #               type1atts=[],
    #               type2atts=['status', 'currency'],
    #               asof='2015-10-23')
    #
    #     for row in self.h5table.iterrows():
    #         dim.update(row)
    #     self.h5dim.flush()
    #
    #     for row in self.h5table.iterrows():
    #         dim.update(row)
    #     self.h5dim.flush()
    #
    #     expected = str((b'1', 10, b'Not Delivered', b'USD',
    #                     0, 1445558400000000000, 7258032000000000000, 1, True))
    #
    #     self.assertEqual(len(self.h5dim), 1)
    #     self.assertEqual(str(self.h5dim[1]), expected)
    #
    # def test_add_new_row(self):
    #     df = pd.read_csv('tests/data/add 1 row.csv')
    #     df['order'] = df['order'].astype(str)
    #     store = pd.HDFStore(self.filename, 'a')
    #     store.append('orders', df, data_columns=True, index=False)
    #     store.close()
    #
    #     dim = scd(connection=self.h5dim,
    #               lookupatts=['order', 'line'],
    #               type1atts=[],
    #               type2atts=['status', 'currency'],
    #               asof='2015-10-23')
    #
    #     for row in self.h5table.iterrows():
    #         print(row[:])
    #         # dim.update(row)
    #     # self.h5dim.flush()
    #
    #     expected = str((b'1', 20, b'Completed', b'USD',
    #                     0, 1445558400000000000, 7258032000000000000, 1, True))
    #
    #     self.assertEqual(len(self.h5dim), 2)
    #     self.assertEqual(str(self.h5dim[1]), expected)
