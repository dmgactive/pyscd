# -*- coding: utf-8 -*-

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
                 minfrom='1970-01-01',
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
        self.minfrom = pd.to_datetime([minfrom]).astype(np.int64)[0]
        self.maxto = pd.to_datetime([maxto]).astype(np.int64)[0]

        # Create the conditions that we will need

        self._v_types = self.connection.description._v_types
        self._v_string_type = [k for k, v in self._v_types.items()
                                    if v == 'string']
        self.keylookupcondition = ' & '.join(['({!s} == _{!s})'.\
            format(att, att) for att in self.lookupatts])
        self.keylookupcondition += ' & ({!s} == {!s})'.\
            format(self.currentatt, True)

        try:
            self.__maxid = connection[-1:][self.key][0]
        except IndexError:
            self.__maxid = -1

    # def quote(self, attribute):
    #     if attribute in self._v_string_type:
    #         return "'{!s}'".format(attribute)
    #     else:
    #         return attribute

    def lookup(self, row):
        """Find the key for the newest version of the row.
        """
        condvars = {'_' + att: row[att] for att in self.lookupatts}
        key = self.connection.get_where_list(self.keylookupcondition, condvars)
        if len(key) > 0:
            return key[0]
        return None

    def update(self, tablerow):
        """Update the dimension by inserting new rows and adding a new versions
           of modified rows.
        """

        # Get the newest version
        key = self.lookup(tablerow)
        if key is None:
            # It is a new member. We add the first version.
            row = self.connection.row
            row[self.key] = self._getnextid()
            row[self.fromatt] = self.minfrom
            row[self.toatt] = self.maxto
            row[self.versionatt] = 1
            row[self.currentatt] = True

            for col in self.attributes:
                row[col] = tablerow[col]

            row.append()

    def _getnextid(self):
        self.__maxid += 1
        return self.__maxid
