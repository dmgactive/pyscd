# -*- coding: utf-8 -*-

import datetime
import pandas as pd
import numpy as np
import tables as tb


class SlowlyChangingDimension(object):
    def __init__(self, connection,
                 lookupatts, type1atts, type2atts,
                 key='scd_id',
                 fromatt='scd_valid_from',
                 toatt='scd_valid_to',
                 versionatt='scd_version',
                 currentatt='scd_current',
                 asof=None,
                 maxto='2199-12-31'):
        if not isinstance(key, str):
            raise ValueError('Key argument must be a string')
        if not isinstance(lookupatts, list) or not len(lookupatts):
            raise ValueError('No natural key given')
        if not isinstance(type1atts, list):
            raise ValueError('Type 1 attributes argument must be a list')
        if not isinstance(type2atts, list):
            raise ValueError('Type 2 attributes argument must be a list')
        if not isinstance(connection, tb.Table):
            raise TypeError('Connection argument must point to a PyTables table')

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

        if not asof:
            today = datetime.date.today()
            self.asof = pd.to_datetime(today).astype(np.int64)[0]
        else:
            self.asof = pd.to_datetime([asof],
                yearfirst=True, dayfirst=False).astype(np.int64)[0]

        self.maxto = pd.to_datetime([maxto],
            yearfirst=True, dayfirst=False).astype(np.int64)[0]

        # Create the conditions that we will need

        self._v_types = self.connection.description._v_types
        self._v_string_type = [k for k, v in self._v_types.items()
                                    if v == 'string']
        self.keylookupcondition = ' & '.join(['({!s} == _{!s})'.\
            format(att, att) for att in self.lookupatts])
        self.keylookupcondition += ' & ({!s} == {!s})'.\
            format(self.currentatt, True)

        self.allkeyscondition = ' & '.join(['({!s} == _{!s})'.\
            format(att, att) for att in self.lookupatts])

        try:
            self.__maxid = connection[-1:][self.key][0]
        except IndexError:
            self.__maxid = -1

    def __exit__(self):
        self.connection.flush()

    def lookup(self, tablerow):
        """Find the key for the newest version of the row.
        """
        condvars = {'_' + att: tablerow[att] for att in self.lookupatts}
        row = self.connection.read_where(self.keylookupcondition, condvars)
        if row:
            return row
        return None

    def getrowbykey(self, key):
        row = self.connection.read_where('({!s} == key)'.format(self.key),
                                        {'key': key})
        return row

    def update(self, row):
        """Update the dimension by inserting new rows and adding a new version
           of modified rows.
        """
        # Get the newest version
        other = self.lookup(row)
        if other is None:
            # It is a new member. We add the first version.
            self.insert(row)
        else:
            # There is an existing version. Check if the attributes are
            # identical.
            print(row[:])
            print(other[:])

            # Check if any type 1 attribute was modified
            for att in self.type1atts:
                if row[att] != other[att]:
                    self.__perform_type1_updates(row, other)
                    break

            # Check if any type 2 attribute was modified
            for att in self.type2atts:
                if row[att] != other[att]:
                    self.__track_type2_history(row, other)
                    break

    def insert(self, newrow):
        """Insert the given row.
        """
        row = self.connection.row

        # Fill new row columns
        for col in self.attributes:
            row[col] = newrow[col]

        # Fill SCD columns
        row[self.key] = self._getnextid()
        row[self.fromatt] = self.asof
        row[self.toatt] = self.maxto
        row[self.versionatt] = 1
        row[self.currentatt] = True

        row.append()

    def __perform_type1_updates(self, tablerow, other):
        """Find and update all rows with same Lookup Attributes.
        """
        condvars = {'_' + att: tablerow[att] for att in self.lookupatts}
        for row in self.connection.where(self.allkeyscondition, condvars):
            for att in self.type1atts:
                row[att] = tablerow[att]
            row.update()

    def __track_type2_history(self, row, other):
        pass

    def _getnextid(self):
        self.__maxid += 1
        return self.__maxid
