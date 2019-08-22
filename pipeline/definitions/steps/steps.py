import os
import hashlib
import pandas as pd

from pipeline.registry import registry
from database.db import db

from psycopg2.extensions import AsIs
from collections import defaultdict


class Step:
    """
    Discrete step in pipeline
    """
    def __init__(self, *args):
        pass

    def execute(self, *args):
        pass


def _calculate_md5(file):
    hash_md5 = hashlib.md5()
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class ScanDataSource(Step):
    """reads and passes all objects in specified path location"""

    def __init__(self, source_path, file_format):
        self.source_path = source_path
        self.file_format = file_format

    def execute(self, context):
        for _, _, filenames in os.walk(f"{self.source_path}"):

            filepaths = [f"{self.source_path}/{filename}" for
                                    filename in filenames if filename.endswith(self.file_format)]
            print(filepaths)
            return filepaths


class CheckForNewDelivery(Step):
    """checks for md5 hash for files in the file registry and filters
    only new files to allow for incremental data ingestion"""

    def __init__(self, table):
        self.table = table

    def execute(self, filepaths):
        new_files = []
        key_list = []
        """calculate md5 hash of each file and reference registry to see if we have already processed this file"""

        for file in filepaths:
            md5_hex = _calculate_md5(file)
            # Check if file has already been processed by pipeline
            if not registry.exists(self.table, "delivery", md5_hex):
                key_list.append(md5_hex)
                new_files.append(file)

        print(f"\nLOG: New files detected for ingestion:\n{new_files}")

        return new_files


class CheckForRowUpdates:

    """Checks primary keys of new deliveries against stored keys, in case a row needs to be updated,
    splits dataframe intos rows that should be updates and new rows to be added"""

    def __init__(self, table):
        self.table = table

    def execute(self, df):
        df = df
        updates = set()
        for idx in df.index:
            if registry.exists(self.table, "row", idx):
                updates.add(idx)

        update_df = df.loc[df.index.isin(updates)]
        new_df = df.loc[~df.index.isin(updates)]

        return update_df, new_df


class ReadAsDataframe(Step):

    def __init__(self,  index, index_dtype={}, custom_read=None):
        self.custom_read = custom_read
        self.index = index
        self.index_dtype = index_dtype

    def execute(self, new_files):

        li = []
        for fp in new_files:
            if self.custom_read:
                df = self.custom_read(fp)

            elif fp.endswith("csv"):
                df = pd.read_csv(fp)

            elif fp.endswith("json"):
                df = pd.read_json(fp)

            else:
                raise Exception("Invalid file format")

            df["delivery_key"] = _calculate_md5(fp)
            li.append(df)

        df = pd.concat(li, axis=0)

        for index, dtype in self.index_dtype.items():
            df[index] = df[index].astype(dtype)

        df = df.set_index(self.index)
        return df


class ToCSV(Step):

    def __init__(self, write_path):
        self.write_path = write_path

    def execute(self, df):
        print(f"LOG: writing dataframe out to {self.write_path}")
        df.to_csv(self.write_path)
        return df


class ToPostgres(Step):

    def __init__(self, table):
        self.table = table

    def execute(self, dfs):
        update_df, new_df = dfs
        db.df_to_postgres(new_df.reset_index(), self.table)

        index_names = update_df.index.names
        for idx, row in update_df.iterrows():
                index_vals = list(zip(index_names, idx))
                query = f"UPDATE {self.table} SET " + ', '.join(c + " = '{0}'".format(str(v).replace("'", "\"+"''))
                                                                for c, v in row.iteritems())

                query += f" WHERE " + ' AND '.join(c + " = '{0}'".format(str(idx).replace("'", "\'"))
                                                   for c, idx, in index_vals)
                db.update_postgres_table(query)

        return new_df


class RegisterKeys(Step):

    def __init__(self, table):
        self.table = table

    def execute(self, df):
        for key in df['delivery_key'].unique().tolist():
            registry.register_key(self.table, "delivery", key)

        for idx in df.index:
            registry.register_key(self.table, "row", idx)


class QueryPostgres(Step):

    def __init__(self, select, query, index):
        self.select = select
        self.query = query
        self.index = index

    def execute(self, context):

        query = self.query % (AsIs(','.join(self.select)),)

        if "dependency map" in context:
            query += " where "
            query += " and ".join(f"{pipeline}.delivery_key IN (" +
                                  '.'.join("'{0}'".format(d) for d in deliveries) + ")"
                                  for pipeline, deliveries in context["dependency map"].items())

        df = db.postgres_to_df(query)
        df = df.set_index(self.index)
        return df


class CheckDependencies(Step):

    def __init__(self, dependents, table):
        self.dependents = dependents
        self.table = table

    def execute(self, _):
        print("LOG: New dependent deliveries:")
        dep_map = defaultdict(list)
        for dep in self.dependents:
            for delivery_id in registry.retrieve_keys(dep.table, "delivery"):
                if not registry.exists(self.table, "delivery", delivery_id):
                    print(f"      table: {self.table} delivery: {delivery_id}")
                    dep_map[dep.table].append(delivery_id)

        return dep_map
