from pipeline.definitions.steps.steps import ScanDataSource, CheckForNewDelivery, ToPostgres, \
    ReadAsDataframe, CheckForRowUpdates, RegisterKeys
from pipeline.definitions.pipeline_definition import Pipeline


name = "CustomerPipeline"
table = "customers"
CustomerPipeline = Pipeline(
    name=name,
    table=table,
    steps=[
        ScanDataSource(source_path="datasource/customers", file_format="csv"),
        CheckForNewDelivery(table=table),
        ReadAsDataframe(index="customer_id"),
        CheckForRowUpdates(table=table),
        ToPostgres(table=table),
        RegisterKeys(table=table)
    ])
