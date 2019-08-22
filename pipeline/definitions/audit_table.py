from pipeline.definitions.steps.steps import QueryPostgres, CheckDependencies, ToCSV
from pipeline.definitions.pipeline_definition import Pipeline
from pipeline.definitions.orders_summary import OrdersSummaryPipeline
from datetime import datetime

select = ['*']

query = 'select %s from orders_summary_audit'

name = "OrdersSummaryAuditPipeline"
table = "orders_summary_audit"
OrdersSummaryPipeline = Pipeline(
    name=name,
    table=table,
    steps=[
        CheckDependencies(dependents=[OrdersSummaryPipeline], table=table),
        QueryPostgres(select=select, query=query, index="audit_ts"),
        ToCSV(write_path=f"output/order_summary_audit/{datetime.now().strftime('%m-%d-%Y-%H:%M:%S')}.csv")
    ])