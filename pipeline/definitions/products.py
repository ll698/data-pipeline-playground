from pipeline.definitions.steps.steps import ScanDataSource, CheckForNewDelivery, \
    ToPostgres, ReadAsDataframe, CheckForRowUpdates, RegisterKeys
from pipeline.definitions.pipeline_definition import Pipeline


name = "ProductsPipeline"
table = "products"
ProductPipeline = Pipeline(
    name=name,
    table=table,
    steps=[
        ScanDataSource(source_path="datasource/products", file_format="csv"),
        CheckForNewDelivery(table=table),
        ReadAsDataframe(index="product_id"),
        CheckForRowUpdates(table=table),
        ToPostgres(table=table),
        RegisterKeys(table=table)
    ])
