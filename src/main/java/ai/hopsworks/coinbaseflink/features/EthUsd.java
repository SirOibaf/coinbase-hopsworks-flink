package ai.hopsworks.coinbaseflink.features;

import ai.hopsworks.coinbaseflink.utils.Ticker;
import ai.hopsworks.coinbaseflink.utils.WSReader;
import com.logicalclocks.hsfs.flink.FeatureStore;
import com.logicalclocks.hsfs.flink.HopsworksConnection;
import com.logicalclocks.hsfs.flink.StreamFeatureGroup;
import com.twitter.chill.java.UnmodifiableCollectionSerializer;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class EthUsd {

  public static final int CHECKPOINTING_INTERVAL_MS = 5000;
  private static final String JOB_NAME = "Coinbase ETH-USD ticker";

  private StreamFeatureGroup streamFeatureGroup;

  public EthUsd() throws Exception {
    HopsworksConnection hopsworksConnection = HopsworksConnection.builder()
        .host("41074450-096b-11ee-96ca-a76595e0c7d6.cloud.hopsworks.ai")
        .project("eth_flink")
        .apiKeyValue("QJYQctfVQph3C4Vv.PczprNXHdfWMhJHidnVEq84aVg1wNHG4fkTx9mrERusMZfMSFoqiihkfPZx9UuDw")
        .build();

    FeatureStore featureStore = hopsworksConnection.getFeatureStore();
    streamFeatureGroup = featureStore.getStreamFeatureGroup("eth_usd_price_5_min", 1);
  }

  public void stream() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<Price5Minutes> websocketStream = env.addSource(new WSReader())
        .keyBy(Ticker::getTicker)
        .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
        .aggregate(new PriceAccumulators());

    streamFeatureGroup.insertStream(websocketStream);

    env.execute(JOB_NAME);
    env.enableCheckpointing(CHECKPOINTING_INTERVAL_MS);
    env.setRestartStrategy(RestartStrategies.noRestart());
  }
}
