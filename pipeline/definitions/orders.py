from pipeline.definitions.steps.steps import ScanDataSource, CheckForNewDelivery, ToPostgres, \
    ReadAsDataframe, CheckForRowUpdates, RegisterKeys
from pipeline.definitions.pipeline_definition import Pipeline


name="OrdersPipeline"
table = "orders"
OrdersPipeline = Pipeline(
    name=name,
    table=table,

    steps=[
        ScanDataSource(source_path="datasource/orders", file_format="csv"),
        CheckForNewDelivery(table=table),
        ReadAsDataframe(index="order_id"),
        CheckForRowUpdates(table=table),
        ToPostgres(table=table),
        RegisterKeys(table=table)
    ])