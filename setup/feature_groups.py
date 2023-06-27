# Setup the feature groups for the Flink pipelines
import hsfs
from hsfs.feature import Feature

connection = hsfs.connection()
fs = connection.get_feature_store()

# Window aggregations
price_window_minutes = [5, 10, 60]
price_window_fg_name_pattern = "eth_usd_price_{}_min"
price_window_fg_version = 1

# Price Features
features = [
    Feature(name="ticker", type="string"),
    Feature(name="timestamp", type="timestamp"),
    Feature(name="price_avg", type="float"),
    Feature(name="price_max", type="float"),
    Feature(name="price_min", type="float"),
]

for window in price_window_minutes:
    fg = fs.create_feature_group(
        name=price_window_fg_name_pattern.format(window),
        version=price_window_fg_version,
        primary_key=["ticker"],
        event_time="timestamp",
        statistics_config=False,
        online_enabled=True,
    )

    fg.save(features)
