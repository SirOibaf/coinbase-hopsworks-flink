from hsfs.feature import Feature
import great_expectations as ge

import hopsworks


project = hopsworks.login(
    host='10.87.43.126',
    port=443,
    project='test',
    engine='python', # spark-no-metastore
    api_key_value='Q4ilDVggoRQyvj3O.sHnWx1SltUELcsO7q6bgbW12RGUaX6FPOFNty8Aj2IP8SDuGEHfOMJPscg99ElUr',
    hostname_verification=False,
)

fs = project.get_feature_store()

# Window aggregations
price_window_minutes = [5, 10, 60]
price_window_fg_name_pattern = "eth_usd_price_{}_min"
price_window_fg_version = 6

# Price Features
features = [
    Feature(name="ticker", type="string"),
    Feature(name="timestamp", type="bigint"),
    Feature(name="price_avg", type="float"),
    Feature(name="price_max", type="float"),
    Feature(name="price_min", type="float"),
    # Feature(name="events", type="array<struct<sku:string,timestamp:bigint>>"),
]

expectation_suite = ge.core.ExpectationSuite(
    expectation_suite_name="validate_on_insert_suite"
)

expectation_suite.add_expectation(
    ge.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={
            "column": "price_avg",
            "min_value": -1,
            "max_value": 0,
        }
    )
)

for window in price_window_minutes:
    fg = fs.create_feature_group(
        name=price_window_fg_name_pattern.format(window),
        version=price_window_fg_version,
        primary_key=["ticker"],
        event_time="timestamp",
        statistics_config=False,
        online_enabled=True,
        expectation_suite=expectation_suite
    )

    fg.save(features)
