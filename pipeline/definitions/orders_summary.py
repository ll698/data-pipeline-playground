from pipeline.definitions.steps.steps import QueryPostgres, CheckDependencies, \
    ToCSV, CheckForRowUpdates, ToPostgres, RegisterKeys
from pipeline.definitions.pipeline_definition import Pipeline
from pipeline.definitions.orders import OrdersPipeline
from datetime import datetime

select = [
    "orders.*,"
    "(orders.shipped_date is not null) as is_shipped,"
    "(orders.shipped_date < orders.required_date) as is_shipped_on_time,"
    "(order_details.discount * order_details.quantity) as total_discount,"
    "(((order_details.unit_price - order_details.discount) * order_details.quantity) + orders.freight) as total_price"
    ]

query = 'select %s from orders inner join order_details on orders.order_id = order_details.order_id'

name = "OrdersSummaryPipeline"
table = "orders_summary"
OrdersSummaryPipeline = Pipeline(
    name=name,
    table=table,
    steps=[
        CheckDependencies(dependents=[OrdersPipeline], table=table),
        QueryPostgres(select=select, query=query, index="order_id"),
        ToCSV(write_path=f"output/order_summary/{datetime.now().strftime('%m-%d-%Y-%H:%M:%S')}.csv"),
        CheckForRowUpdates(table=table),
        ToPostgres(table=table),
        RegisterKeys(table=table)
    ])
