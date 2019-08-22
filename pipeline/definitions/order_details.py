from pipeline.definitions.steps.steps import ScanDataSource, CheckForNewDelivery, \
    ToPostgres, ReadAsDataframe, CheckForRowUpdates, RegisterKeys, Step
from pipeline.definitions.pipeline_definition import Pipeline
import json
import pandas as pd


def order_details_json_to_df(fp):
    json_file = open(fp, "r")
    json_str = json_file.read()
    json_data = json.loads(json_str)

    print(f"\nLOG: Writing {fp} to order details table")
    row_list = []
    for order_id, rows in json_data.items():
        for row in rows:
            row["order_id"] = order_id
            row_list.append(row)

    df = pd.DataFrame(row_list)
    return df


class ChangeColType(Step):

    def execute(self, df):
        df.quantity = df.quantity.astype("int")
        return df


name = "OrderDetailsPipeline"
table = "order_details"
OrderDetailsPipeline = Pipeline(
    name=name,
    table=table,
    steps=[
        ScanDataSource(source_path="datasource/order_details", file_format="json"),

        CheckForNewDelivery(table=table),

        ReadAsDataframe(custom_read=order_details_json_to_df,
                        index=["product_id", "order_id"],
                        index_dtype={"product_id": int}),
        ChangeColType(),
        CheckForRowUpdates(table=table),
        ToPostgres(table=table),
        RegisterKeys(table=table)
    ])
