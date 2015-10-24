# -*- coding: utf-8 -*-

import unittest
import os
import pandas as pd
import tables as tb
from pyscd.dimension import SlowlyChangingDimension as scd


class DimensionOrders(tb.IsDescription):
    order           = tb.StringCol(255, pos=1)
    line            = tb.Int64Col(pos=2)
    status          = tb.StringCol(255, pos=3)
    currency        = tb.StringCol(255, pos=4)
    scd_id          = tb.Int64Col(pos=5)
    scd_valid_from  = tb.Int64Col(pos=6)
    scd_valid_to    = tb.Int64Col(pos=7)
    scd_version     = tb.Int16Col(pos=8)
    scd_current     = tb.BoolCol(pos=9)


class DimensionWorkCenters(tb.IsDescription):
    workcenter             = tb.StringCol(255, pos=0)
    description            = tb.StringCol(255, pos=1)
    group                  = tb.StringCol(255, pos=2)
    hours                  = tb.Float64Col(pos=3)
    scd_id                 = tb.Int64Col(pos=4)
    scd_valid_from         = tb.Int64Col(pos=5)
    scd_valid_to           = tb.Int64Col(pos=6)
    scd_version            = tb.Int16Col(pos=7)
    scd_current            = tb.BoolCol(pos=8)


def create_dimension_orders(h5file):
    filters = tb.Filters(complevel=9, complib='zlib')
    group = h5file.create_group("/", 'dimorders')
    table = h5file.create_table(group, 'table',
                                DimensionOrders, filters=filters)
    table.cols.order.create_index()


def create_dimension_workcenters(h5file):
    filters = tb.Filters(complevel=9, complib='zlib')
    group = h5file.create_group("/", 'dimworkcenters')
    table = h5file.create_table(group, 'table',
                                DimensionWorkCenters, filters=filters)
    table.cols.workcenter.create_index()


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
        self.filename = 'test.h5'

        if os.path.isfile(self.filename):
            os.remove(self.filename)

        self.h5file = tb.open_file(self.filename, mode='a')

        create_dimension_orders(self.h5file)
        create_dimension_workcenters(self.h5file)

        self.h5file.close()

    def tearDown(self):
        self.h5file.close()
        # if os.path.isfile(self.filename):
        #     os.remove(self.filename)

    def test_import_new_data_fill_scd_columns(self):
        import_orders(self.filename, 'tests/data/orders x1.csv')

        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.orders.table
        h5dim = h5file.root.dimorders.table

        dim = scd(connection=h5dim,
                  lookupatts=['order'],
                  type1atts=[],
                  type2atts=['line', 'status', 'currency'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()

        expected = str((b'1', 10, b'Not Delivered', b'USD',
                        1, 1445558400000000000, 7258032000000000000, 1, True))

        self.assertEqual(len(h5dim), 1)
        self.assertEqual(str(h5dim[0]), expected)

        h5file.close()

    def test_import_same_data_does_not_duplicate_row(self):
        import_orders(self.filename, 'tests/data/orders x1.csv')

        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.orders.table
        h5dim = h5file.root.dimorders.table

        dim = scd(connection=h5dim,
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()

        expected = str((b'1', 10, b'Not Delivered', b'USD',
                        1, 1445558400000000000, 7258032000000000000, 1, True))

        self.assertEqual(len(h5dim), 1)
        self.assertEqual(str(h5dim[0]), expected)

        h5file.close()

    def test_add_new_row(self):
        # Update dimension with first file
        import_orders(self.filename, 'tests/data/orders x1.csv')

        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.orders.table
        h5dim = h5file.root.dimorders.table

        dim = scd(connection=h5dim,
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()
        h5file.close()

        # Update dimension with second file
        import_orders(self.filename, 'tests/data/add 1 row.csv')

        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.orders.table
        h5dim = h5file.root.dimorders.table

        dim = scd(connection=h5dim,
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()

        expected = str((b'1', 20, b'Completed', b'USD',
                        2, 1445558400000000000, 7258032000000000000, 1, True))

        self.assertEqual(len(h5dim), 2)
        self.assertEqual(str(h5dim[1]), expected)

        h5file.close()

    def test_modify_type_1_column(self):
        # Update dimension with first file
        import_orders(self.filename, 'tests/data/add 1 row.csv')

        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.orders.table
        h5dim = h5file.root.dimorders.table

        dim = scd(connection=h5dim,
                  lookupatts=['order', 'line'],
                  type1atts=['status'],
                  type2atts=['currency'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()
        h5file.close()

        # Update dimension with second file
        import_orders(self.filename, 'tests/data/modify 1 row.csv')

        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.orders.table
        h5dim = h5file.root.dimorders.table

        dim = scd(connection=h5dim,
                  lookupatts=['order', 'line'],
                  type1atts=['status'],
                  type2atts=['currency'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()

        expected = str((b'1', 10, b'Completed', b'USD',
                        1, 1445558400000000000, 7258032000000000000, 1, True))

        self.assertEqual(len(h5dim), 2)
        self.assertEqual(str(h5dim[0]), expected)

        h5file.close()

    def test_modify_type_2_column(self):
        # Update dimension with first file
        import_orders(self.filename, 'tests/data/add 1 row.csv')

        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.orders.table
        h5dim = h5file.root.dimorders.table

        dim = scd(connection=h5dim,
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()
        h5file.close()

        # Update dimension with second file
        import_orders(self.filename, 'tests/data/modify 1 row.csv')

        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.orders.table
        h5dim = h5file.root.dimorders.table

        dim = scd(connection=h5dim,
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()

        # Check if one row was inserted
        self.assertEqual(len(h5dim), 3)

        # Check first version row
        expected = str((b'1', 10, b'Not Delivered', b'USD',
                        1, 1445558400000000000, 1445558400000000000, 1, False))
        self.assertEqual(str(h5dim[0]), expected)

        # Check second version row
        expected = str((b'1', 10, b'Completed', b'USD',
                        3, 1445558400000000000, 7258032000000000000, 2, True))
        self.assertEqual(str(h5dim[2]), expected)

        h5file.close()

    def test_update_info_new_rows(self):
        # Update dimension with first file
        import_orders(self.filename, 'tests/data/add 1 row.csv')

        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.orders.table
        h5dim = h5file.root.dimorders.table

        dim = scd(connection=h5dim,
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()

        self.assertEqual(dim.new_rows, 2)
        self.assertEqual(dim.updated_type1_rows, 0)
        self.assertEqual(dim.updated_type2_rows, 0)

        h5file.close()

    def test_update_info_updated_type1_rows(self):
        # Update dimension with first file
        import_orders(self.filename, 'tests/data/add 1 row.csv')

        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.orders.table
        h5dim = h5file.root.dimorders.table

        dim = scd(connection=h5dim,
                  lookupatts=['order', 'line'],
                  type1atts=['status'],
                  type2atts=['currency'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()
        h5file.close()

        # Update dimension with second file
        import_orders(self.filename, 'tests/data/modify 1 row.csv')

        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.orders.table
        h5dim = h5file.root.dimorders.table

        dim = scd(connection=h5dim,
                  lookupatts=['order', 'line'],
                  type1atts=['status'],
                  type2atts=['currency'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()

        self.assertEqual(dim.new_rows, 0)
        self.assertEqual(dim.updated_type1_rows, 1)
        self.assertEqual(dim.updated_type2_rows, 0)

        h5file.close()

    def test_update_info_updated_type2_rows(self):
        # Update dimension with first file
        import_orders(self.filename, 'tests/data/add 1 row.csv')

        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.orders.table
        h5dim = h5file.root.dimorders.table

        dim = scd(connection=h5dim,
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()
        h5file.close()

        # Update dimension with second file
        import_orders(self.filename, 'tests/data/modify 1 row.csv')

        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.orders.table
        h5dim = h5file.root.dimorders.table

        dim = scd(connection=h5dim,
                  lookupatts=['order', 'line'],
                  type1atts=[],
                  type2atts=['status', 'currency'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()

        self.assertEqual(dim.new_rows, 0)
        self.assertEqual(dim.updated_type1_rows, 0)
        self.assertEqual(dim.updated_type2_rows, 1)

        h5file.close()

    def test_import_same_data_several_times_does_nothing(self):
        import_workcenters(self.filename,
                           'tests/data/Centros de trabalho.xlsx',
                           'Centros de Trabalho')

        # Import 1st time
        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.workcenters.table
        h5dim = h5file.root.dimworkcenters.table

        dim = scd(connection=h5dim,
                  lookupatts=['workcenter'],
                  type1atts=[],
                  type2atts=['description', 'group', 'hours'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()

        self.assertEqual(dim.new_rows, 43)
        self.assertEqual(dim.updated_type1_rows, 0)
        self.assertEqual(dim.updated_type2_rows, 0)

        h5file.close()

        # Import 2st time
        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.workcenters.table
        h5dim = h5file.root.dimworkcenters.table

        dim = scd(connection=h5dim,
                  lookupatts=['workcenter'],
                  type1atts=[],
                  type2atts=['description', 'group', 'hours'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()

        self.assertEqual(dim.new_rows, 0)
        self.assertEqual(dim.updated_type1_rows, 0)
        self.assertEqual(dim.updated_type2_rows, 0)

        h5file.close()

        # Import 3st time
        h5file = tb.open_file(self.filename, mode='a')
        h5table = h5file.root.workcenters.table
        h5dim = h5file.root.dimworkcenters.table

        dim = scd(connection=h5dim,
                  lookupatts=['workcenter'],
                  type1atts=[],
                  type2atts=['description', 'group', 'hours'],
                  asof='2015-10-23')

        for row in h5table.iterrows():
            dim.update(row)
        h5dim.flush()

        self.assertEqual(dim.new_rows, 0)
        self.assertEqual(dim.updated_type1_rows, 0)
        self.assertEqual(dim.updated_type2_rows, 0)

        h5file.close()
