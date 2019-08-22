from pipeline.definitions.steps.steps import QueryPostgres, CheckDependencies, ToCSV, CheckForRowUpdates, ToPostgres, Step
from pipeline.definitions.pipeline_definition import Pipeline
from pipeline.definitions.orders_summary import OrdersSummaryPipeline
from datetime import datetime
from database.db import db
import pandas
from pipeline.registry import registry
import pycountry


class CheckForNewOrders(Step):
    def __init__(self, table):
        self.table = table

    def execute(self, df):
        df = df
        new_order = set()
        order_update = set()
        new_customer = set()
        for idx, row in df.iterrows():
            customer_seen = registry.exists(self.table, "customers", row["customer_id"])
            order_seen = registry.exists(self.table, "orders", row["customer_id"])
            if customer_seen and not order_seen:
                new_order.add(idx)
            elif not customer_seen:
                new_customer.add(idx)
            else:
                order_update.add(idx)

        new_order_df = df.loc[df.index.isin(new_order)]
        order_update_df = df.loc[df.index.isin(order_update)]
        new_customer_df = df.loc[df.index.isin(new_customer)]

        return new_order_df, order_update_df, new_customer_df


class UpdateCustomerSummaryPostgres(Step):
    def __init__(self, table):
        self.table = table

    def execute(self, dfs):
        new_order_df, order_update_df, new_customer_df = dfs

        for idx, row in new_order_df.iterrows():
                query = f"UPDATE {self.table} SET total_order_expense = total_order_expense + {row['total_price']}, " \
                        f"SET last_order_date = GREATEST(last_order_date, {row['order_date']}, " \
                        f"SET last_order_address = '{row['ship_address']}', " \
                        f"SET delivery_key = {row['delivery_key']} " \
                        f"WHERE customer_id = idx" \

                db.update_postgres_table(query)

        d1 = new_customer_df.groupby(['customer_id']).agg({'total_price': 'sum', 'order_date': 'max'})
        d2 = new_customer_df[['customer_id', 'order_date',
                              'ship_address', 'ship_city',
                              'ship_region', 'ship_postal_code',
                              'ship_country']].drop_duplicates()

        # add local currency column
        def extract_currency(row):

            # some hardcoding because pycountry doesnt seem to work with european countries due to iso standards
            country = row["ship_country"]
            if country == "USA":
                return "USD"
            if country == "Brazil":
                return "BRL"
            if country == "Venezuela":
                return "VEF"
            if country == "UK":
                return "GBP"

            country = pycountry.countries.get(name=row["ship_country"])
            if not country:
                return "unknown"

            currency = pycountry.currencies.get(numeric=country.numeric)
            if not currency:
                return "EUR"

            return currency.alpha_3

        d2["local_currency"] = d2.apply(lambda row: extract_currency(row), axis=1)
        new_customer_summary = pandas.merge(d1, d2, on=['customer_id', 'order_date'], how='inner')

        new_customer_summary = new_customer_summary.rename(columns={'total_price': 'total_order_expense',
                                                                    'order_date': 'last_order_date',
                                                                    'ship_address': 'last_ship_address',
                                                                    'ship_city': 'last_ship_city',
                                                                    'ship_postal_code': 'last_ship_postal_code'})
        new_customer_summary = new_customer_summary[['customer_id',
                                                     'total_order_expense',
                                                     'last_order_date',
                                                     'last_ship_address',
                                                     'last_ship_city',
                                                     'last_ship_postal_code',
                                                     'local_currency']]
        db.df_to_postgres(new_customer_summary.set_index('customer_id'), 'customer_summary')
        return new_customer_summary


select = ["*"]
query = "select %s from orders_summary"
name = "CustomerSummaryPipeline"
table = "customer_summary"

CustomerSummaryPipeline = Pipeline(
    name=name,
    table=table,
    steps=[
        CheckDependencies(dependents=[OrdersSummaryPipeline], table=table),
        QueryPostgres(select=select, query=query, index=["order_id"]),
        CheckForNewOrders(table=table),
        UpdateCustomerSummaryPostgres(table=table),
        ToCSV(write_path=f"output/customer_summary/{datetime.now().strftime('%m-%d-%Y-%H:%M:%S')}.csv"),
        CheckForRowUpdates(table=table),
    ])
