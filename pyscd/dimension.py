# -*- coding: utf-8 -*-

import datetime
import pandas as pd
import numpy as np
import tables as tb


class SlowlyChangingDimension(object):
    """A class for accessing a slowly changing dimension of types 1 and 2.
    """

    def __init__(self, connection,
                 lookupatts, type1atts, type2atts,
                 key='scd_id',
                 fromatt='scd_valid_from',
                 toatt='scd_valid_to',
                 maxto='2199-12-31',
                 versionatt='scd_version',
                 currentatt='scd_current',
                 asof=None):
        """
        Parameters
        ----------

        connection
            Required. The Tables.Table pointing to this dimension. The table
            should already have exists and include the dimension specific
            columns:
            - scd_id          = tb.Int64Col(pos=0)
            - scd_valid_from  = tb.Int64Col(pos=1)
            - scd_valid_to    = tb.Int64Col(pos=2)
            - scd_version     = tb.Int16Col(pos=3)
            - scd_current     = tb.BoolCol(pos=4)

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

        versionatt
            Optional. String with the of the column to hold the version number.
            Default 'scd_version'.

        currentatt
            Optional. String with the of the column to hold the current version
            status.
            Default 'scd_current'.

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
        if not isinstance(connection, tb.Table):
            raise TypeError('Connection argument must be a PyTables table')

        self.connection = connection
        self.lookupatts = lookupatts
        self.type1atts = type1atts
        self.type2atts = type2atts
        self.attributes = lookupatts + type1atts + type2atts
        self.key = key
        self.fromatt = fromatt
        self.toatt = toatt
        self.versionatt = versionatt
        self.currentatt = currentatt
        # TODO: use hash column to check if row was modified

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

        self._v_string_type = [k for k, v in
                               self.connection.description._v_types.items()
                               if v == 'string']

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
        self.currentkeylookupcondition =\
            self.allkeyslookupcondition +\
            ' & ({!s} == {!s})'.format(self.currentatt, True)

        # Get the last used key
        try:
            # Select the key id of the last row.
            self.__maxid = connection[-1:][self.key][0]
        except IndexError:
            # The table is empty, so we set __maxid to 0
            self.__maxid = 0

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
        condvars = {'_' + att: tablerow[att] for att in self.lookupatts}

        row = self.connection.read_where(
            self.currentkeylookupcondition, condvars)

        if row:
            return row
        return None

    def update(self, row):
        """Update the dimension by inserting new rows, modifying type 1
           attributes and adding a new version of modified rows.
        """
        # Get the newest version
        other = self.lookup(row)

        if other is None:
            # It is a new member. We add the first version.
            self.insert(row)
            self._new_count += 1
        else:
            # There is an existing version. Check if the attributes are
            # identical.

            # Check if any type 1 attribute was modified
            for att in self.type1atts:
                if row[att] != other[att]:
                    self.__perform_type1_updates(row, other)
                    self._type1_modified_count += 1
                    break

            # Check if any type 2 attribute was modified
            for att in self.type2atts:
                if (pd.notnull(row[att]) or pd.notnull(other[att])) \
                    and (row[att] != other[att]):
                    self.__track_type2_history(row, other)
                    self._type2_modified_count += 1
                    break

    def insert(self, rowdata, version=1):
        """Insert the given row.
        """
        row = self.connection.row

        # Fill new row columns
        for col in self.attributes:
            row[col] = rowdata[col]

        # Fill SCD columns
        row[self.key] = self._getnextid()
        row[self.fromatt] = self.asof
        row[self.toatt] = self.maxto
        row[self.versionatt] = version
        row[self.currentatt] = True

        row.append()

    def __perform_type1_updates(self, rowdata, other):
        """Find and update all rows with same Lookup Attributes.
        """
        condvars = self._build_condvars(rowdata)

        # Find coordinates of all rows using lookup columns
        coords = self.connection.get_where_list(
            self.allkeyslookupcondition, condvars)
        rows = self.connection.read_coordinates(coords)

        # Update type 1 attributes
        for type1att in self.type1atts:
            rows[type1att][:] = rowdata[type1att]

        # Update dimension
        self.connection.modify_coordinates(coords, rows)

    def __track_type2_history(self, tablerow, other):
        """Track history of type 2 columns. The following actions are performed:
           - Find the current active row and inactivate it:
             - Set valid to attribute to asof.
             - Set current attribute to False.
           - Insert a new version.
        """
        condvars = self._build_condvars(tablerow)

        # Find coordinates of the current row using lookup columns
        coord = self.connection.get_where_list(
            self.currentkeylookupcondition, condvars)
        row = self.connection.read_coordinates(coord)

        # Update valid to and current columns
        row[self.toatt] = self.asof
        row[self.currentatt] = False

        # Update dimension
        self.connection.modify_coordinates(coord, row)

        # Insert new version of the row
        self.insert(tablerow, version=other[self.versionatt] + 1)

    def _getnextid(self):
        self.__maxid += 1
        return self.__maxid

    def _build_condvars(self, row):
        # Build the dict to be used as condvars of a query, like this:
        # {order: _order, line: _line}
        # This dict can then passed to PyTables Table.where()
        # and Table.get_where_list() functions
        # Source: http://www.pytables.org/usersguide/libref/structured_storage.html#tables.Table.where
        condvars = {'_' + att: row[att] for att in self.lookupatts}
        return condvars
