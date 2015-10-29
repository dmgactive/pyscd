# -*- coding: utf-8 -*-

import datetime
import pandas as pd
import numpy as np
import tables as tb
import hashlib
import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


class SlowlyChangingDimension(object):
    """A class for accessing a slowly changing dimension of types 1 and 2.
    """

    def __init__(self, connection, name,
                 lookupatts, type1atts, type2atts,
                 key='scd_id',
                 fromatt='scd_valid_from',
                 toatt='scd_valid_to',
                 maxto='2199-12-31',
                 currentatt='scd_current',
                 hashatt='scd_hash',
                 asof=None,
                 verbose=True):
        """
        Parameters
        ----------

        connection
            Required. The pandas.HDFStore pointing to this dimension.

        name
            Required. The name of the dimension to be stored in the file.

        lookupatts
            Required. A list of the columns that uniquely identify a dimension
            member.

        type1atts
            Required. A list of the columns that should have type1 updates
            applied.

        type2atts
            Required. A list of the columns that should have version tracking.

        key
            Optional. String with the name of the primary key of the dimension
            table.

        fromatt
            Optional. String with the name of the column telling from when the
            version becomes valid.
            Default 'scd_valid_from'.

        toatt
            Optional. String with the name of the column telling ultil when the
            version is valid.
            Default 'scd_valid_to'.

        maxto
            Optional. The date to use for toatt for new versions. Must be a date
            string in the format 'yyyy-MM-dd'.
            Default '2199-12-31'.

        currentatt
            Optional. String with the name of the column to hold the current
            version status.
            Default 'scd_current'.

        hashatt
            Optional. String with the name of the column to hold the hash of
            the attributes of each row.
            Default 'scd_hash'.

        asof
            Optional. The date to use for fromatt for the 1st version of a row.
            * None: Uses current date.
            * String: Uses this value. Must be a date string in the format
                      'yyyy-MM-dd'.
            Default None.
        """
        if not isinstance(key, str):
            raise ValueError('Key argument must be a string')
        if not isinstance(lookupatts, list) or not len(lookupatts):
            raise ValueError('No lookup key(s) given')
        if not isinstance(type1atts, list):
            raise ValueError('Type 1 attributes argument must be a list')
        if not isinstance(type2atts, list):
            raise ValueError('Type 2 attributes argument must be a list')
        if not isinstance(connection, pd.HDFStore):
            raise TypeError('store argument must be a pandas HDFStore')

        self.connection = connection
        self.name = name
        self.lookupatts = lookupatts
        self.type1atts = type1atts
        self.type2atts = type2atts
        self.attributes = lookupatts + type1atts + type2atts
        self.key = key
        self.fromatt = fromatt
        self.toatt = toatt
        self.currentatt = currentatt
        self.hashatt = hashatt
        self.verbose = verbose

        if not asof:
            today = datetime.date.today()
            self.asof = pd.to_datetime([today]).astype(np.int64)[0]
        else:
            self.asof = pd.to_datetime([asof],
                yearfirst=True, dayfirst=False).astype(np.int64)[0]

        self.maxto = pd.to_datetime([maxto],
            yearfirst=True, dayfirst=False).astype(np.int64)[0]

        # Initialize updated count info
        self._new_count = 0
        self._type1_modified_count = 0
        self._type2_modified_count = 0

        self.exists = self.name in self.connection

        if self.exists:
            self.__maxid = self.connection[self.name][self.key].max()
            self._load_cache()
        else:
            self.__maxid = 0
            self.__currentindex = pd.DataFrame()

        # Create the conditions that we will need

        # This gives (lookupatt1 == _lookupatt1)
        #          & (lookupatt2 == _lookupatt2)
        #          & ...
        self.allkeyslookupcondition = ' & '.join(['({!s} == _{!s})'.\
            format(att, att) for att in self.lookupatts])

        # This gives (lookupatt1 == _lookupatt1)
        #          & (lookupatt2 == _lookupatt2)
        #          & ...
        #          & (currentatt == True)
        self.currentkeylookupcondition = self.allkeyslookupcondition +\
            ' & ({!s} == True)'.format(self.currentatt)

    def __exit__(self):
        self.connection.flush()

    @property
    def new_rows(self):
        return self._new_count

    @property
    def updated_type1_rows(self):
        return self._type1_modified_count

    @property
    def updated_type2_rows(self):
        return self._type2_modified_count

    def lookup(self, tablerow):
        """Read the newest version of the row.
        """
        return None

    def update(self, df):
        """Update the dimension by inserting new rows, modifying type 1
           attributes and adding a new version of modified rows.
        """
        if not self.exists:
            # If dimension table does not exists, just insert it for the
            # first time. HDFStore.append will create it automatically.
            self.insert(df.copy())
        else:
            # Computes hash using attributes columns
            df[self.hashatt] = df.apply(lambda x: self._compute_hash(x),
                                        axis=1)

            # Set lookup attributes as index
            df.set_index(self.lookupatts, inplace=True)

            # Find the rows that do not exists in the preloaded index
            new = df.loc[~df.index.isin(self.__currentindex.index)]
            if not new.empty:
                # Insert the first version of new rows.
                self.insert(new.reset_index().copy())

            if self.type1atts:
                # Find the rows that exists in the preloaded index, but with a
                # different hash.
                modified = df.merge(self.__currentindex,
                    left_index=True, right_index=True)
                modified = modified.loc[
                    modified[self.hashatt + '_x'] != modified[self.hashatt + '_y']]
                modified.reset_index(inplace=True)

                if not modified.empty:
                    self.__perform_type1_updates(modified[self.attributes])

            if self.type2atts:
                # Find the rows that exists in the preloaded index, but with a
                # different hash.
                modified = df.merge(self.__currentindex,
                    left_index=True, right_index=True)
                modified = modified.loc[
                    modified[self.hashatt + '_x'] != modified[self.hashatt + '_y']]
                modified.reset_index(inplace=True)

                if not modified.empty:
                    self.__track_type2_history(modified[self.attributes])

    def insert(self, df):
        """Insert the given row.
        """
        # Fill SCD columns
        df[self.key] = 0
        df[self.key] = df[self.key].apply(lambda x: self._getnextid())
        df[self.fromatt] = self.asof
        df[self.toatt] = self.maxto
        df[self.currentatt] = True
        df[self.hashatt] = df.apply(lambda x: self._compute_hash(x),
                                    axis=1)

        self.connection.append(self.name, df, min_itemsize=255, data_columns=True)

    def __perform_type1_updates(self, modified):
        """Find and update all rows with same Lookup Attributes.
        """
        # Open file
        h5file = tb.open_file(self.connection.filename, mode='a')

        # Open table
        table = h5file.get_node('/{!s}/table'.format(self.name))

        # For each row in DataFrame...
        for row in modified.itertuples(index=False):
            rowdata = dict(zip(modified.columns, row))

            # Find all coordinates of these lookup attributes
            condvars = self._build_condvars(rowdata)
            coords = table.get_where_list(
                self.allkeyslookupcondition, condvars)

            # Read all existing rows of these lookup attributes
            tablerows = table.read_coordinates(coords)

            # Update rows type 1 attributes
            for type1att in self.type1atts:
                 tablerows[type1att][:] = rowdata[type1att]

            # Update hash
            for tablerow in tablerows:
                tablerow[self.hashatt] = self._compute_hash(rowdata)

            # Update dimension table
            table.modify_coordinates(coords, tablerows)

        h5file.flush()
        h5file.close()
        del h5file

        self._load_cache()

    def __track_type2_history(self, modified):
        """Track history of type 2 columns. The following actions are performed:
           - Find the current active row and inactivate it:
             - Set valid to attribute to asof.
             - Set current attribute to False.
           - Insert a new version.
        """
        # Open file
        h5file = tb.open_file(self.connection.filename, mode='a')

        # Open table
        table = h5file.get_node('/{!s}/table'.format(self.name))

        # For each row in DataFrame...
        for row in modified.itertuples(index=False):
            rowdata = dict(zip(modified.columns, row))

            # Find coordinate of current version using the lookup attributes
            condvars = self._build_condvars(rowdata)
            coord = table.get_where_list(
                self.currentkeylookupcondition, condvars)

            # Read the current version
            tablerow = table.read_coordinates(coord)

            # Expire the old version
            tablerow[self.toatt] = self.asof
            tablerow[self.currentatt] = False

            # Update dimension table
            table.modify_coordinates(coord, tablerow)

        h5file.flush()
        h5file.close()
        del h5file

        # Insert the new version of updated rows
        self.insert(modified.copy())


    def _getnextid(self):
        self.__maxid += 1
        return self.__maxid

    def _compute_hash(self, row):
        """Computes hash of the entire row.
           See hashlib.algorithms_guaranteed for the complete algorithm list.
        """
        m = hashlib.sha1()

        # self.attributes is a list, so the hash will always be updated in
        # the same order.
        for att in self.attributes:
            value = str(row[att]).encode()
            m.update(value)
        return m.hexdigest()

    def _load_cache(self):
        self.__currentindex = self._get_current_indexes()

    def _get_current_indexes(self):
        """Make a DataFrame with the lookup attributes and hash of all currently
           active rows.
        """
        currentindexes = self.connection.select(
            self.name, where='{!s}=True'.format(self.currentatt),
            columns=self.lookupatts + [self.hashatt])
        currentindexes.set_index(self.lookupatts, inplace=True)
        return currentindexes

    def _build_condvars(self, row):
        """Build the dict to be used as condvars of a query, like this:
           {_order: order, _line: line}

           This dict can then passed to PyTables Table.where()
           and Table.get_where_list() functions

           Source: http://www.pytables.org/usersguide/libref/structured_storage.html#tables.Table.where
        """
        condvars = {'_' + att: row[att] for att in self.lookupatts}
        return condvars
