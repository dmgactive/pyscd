# -*- coding: utf-8 -*-

import datetime
import pandas as pd
import numpy as np
import tables as tb
import hashlib
from collections import defaultdict
from pyscd.progress import Progress
import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


class SlowlyChangingDimension(object):
    """A class for accessing a slowly changing dimension of types 1 and 2.
    """

    def __init__(self, store, name,
                 lookupatts, type1atts, type2atts,
                 key='scd_id',
                 fromatt='scd_valid_from',
                 toatt='scd_valid_to',
                 maxto='2199-12-31',
                 versionatt='scd_version',
                 currentatt='scd_current',
                 hashatt='scd_hash',
                 asof=None,
                 verbose=True):
        """
        Parameters
        ----------

        store
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

        versionatt
            Optional. String with the name of the column to hold the version
            number.
            Default 'scd_version'.

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
        if not isinstance(store, pd.HDFStore):
            raise TypeError('store argument must be a pandas HDFStore')

        self.store = store
        self.name = name
        self.lookupatts = lookupatts
        self.type1atts = type1atts
        self.type2atts = type2atts
        self.attributes = lookupatts + type1atts + type2atts
        self.key = key
        self.fromatt = fromatt
        self.toatt = toatt
        self.versionatt = versionatt
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

        self.exists = self.name in self.store

        if self.exists:
            self.__maxid = self.store[self.name][self.key].max()
            self.__currentindex = self._get_current_indexes()
        else:
            self.__maxid = 0
            self.__currentindex = pd.DataFrame()

    def __exit__(self):
        self.store.flush()

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
            self.insert(df)
        else:
            df[self.hashatt] = df[self.attributes].apply(lambda x:
                self._compute_hash(x), axis=1)

            df.set_index(self.lookupatts, inplace=True)

            new = df.loc[~df.index.isin(self.__currentindex.index)]
            if not new.empty:
                self.insert(new.reset_index())

            modified = df.merge(self.__currentindex,
                left_index=True, right_index=True)
            modified = modified.loc[
                modified[self.hashatt + '_x'] != modified[self.hashatt + '_y']]

            if not modified.empty:
                print('TEVE MODIFICAÇÃO')
                print(modified)
                self.__perform_type1_updates(modified)
                self.__track_type2_history(modified)


    def insert(self, df, version=1):
        """Insert the given row.
        """
        hashvalue = self._compute_hash(df)

        # Fill SCD columns
        df[self.key] = self._getnextid()
        df[self.fromatt] = self.asof
        df[self.toatt] = self.maxto
        df[self.versionatt] = version
        df[self.currentatt] = True
        df[self.hashatt] = df[self.attributes].apply(lambda x:
            self._compute_hash(x), axis=1)

        self.store.append(self.name, df, min_itemsize=255, data_columns=True)


    def __perform_type1_updates(self, modified):
        """Find and update all rows with same Lookup Attributes.
        """
        pass

    def __track_type2_history(self, modified):
        """Track history of type 2 columns. The following actions are performed:
           - Find the current active row and inactivate it:
             - Set valid to attribute to asof.
             - Set current attribute to False.
           - Insert a new version.
        """
        pass

    def _getnextid(self):
        self.__maxid += 1
        return self.__maxid

    def _compute_hash(self, df):
        """Computes hash of the entire row.
           See hashlib.algorithms_guaranteed for the complete algorithm list.
        """
        value = repr(df.values).encode()
        return hashlib.sha1(value).hexdigest()

    def _get_current_indexes(self):
        """Make a DataFrame with the lookup attributes and hash of all currently
           active rows.
        """
        currentindexes = self.store.select(
            self.name, where='{!s}=True'.format(self.currentatt),
            columns=self.lookupatts + [self.hashatt])
        currentindexes.set_index(self.lookupatts, inplace=True)
        return currentindexes
