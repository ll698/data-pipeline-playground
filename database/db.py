import psycopg2
import atexit
from io import StringIO
from sqlalchemy import create_engine
import pandas

"""Code sourced from http://www.postgresqltutorial.com/postgresql-python/connect/"""

db_params = {
    "host": "localhost",
    "database": "datalake",
    "user": "data_pipeline",
    "password": "data-pipeline-password"
}


def cleanup(conn):
    conn.close()


class PostGRES:

    def __init__(self):
        self.conn = self._connect()
        self.engine = create_engine('postgresql+psycopg2://data_pipeline:data-pipeline-password@localhost:5432/datalake')
        atexit.register(cleanup, self.conn)

    @staticmethod
    def _connect():
        """ Connect to the PostgreSQL database server """
        try:
            # connect to the PostgreSQL server
            print('\nLOG: Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**db_params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            print('\nLOG: PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(f'\nLOG: {db_version}')

            return conn

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def df_to_postgres(self, df, write_table):
        """faster way to insert pandas dataframe to postgres table
        code from: https://www.reddit.com/r/Python/comments/690j1q/faster_loading_of_dataframes_from_pandas_to/"""
        engine = self.engine
        output = StringIO()
        # ignore the index
        df.to_csv(output, sep='\t', header=False, index=False)
        output.getvalue()
        # jump to start of stream
        output.seek(0)

        connection = engine.raw_connection()
        cursor = connection.cursor()
        # null values become ''
        cursor.copy_from(output, write_table, null="", columns=df.columns)
        connection.commit()
        cursor.close()

    def postgres_to_df(self, query):
        df = pandas.read_sql_query(query, self.engine)
        return df

    def update_postgres_table(self, query):
        cur = self.conn.cursor()
        cur.execute(query)
        self.commit()

    def cursor(self):
        return self.conn.cursor()

    def close(self):
        self.conn.close()
        print("DB connection closed")

    def commit(self):
        self.conn.commit()


db = PostGRES()
