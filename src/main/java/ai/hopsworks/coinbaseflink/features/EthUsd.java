package ai.hopsworks.coinbaseflink.features;

import ai.hopsworks.coinbaseflink.utils.ConfigKeys;
import ai.hopsworks.coinbaseflink.utils.WSReader;
import com.logicalclocks.hsfs.flink.FeatureStore;
import com.logicalclocks.hsfs.flink.HopsworksConnection;
import com.logicalclocks.hsfs.flink.StreamFeatureGroup;
import com.twitter.chill.java.UnmodifiableCollectionSerializer;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

import java.util.Map;

public class EthUsd {

  public static final int CHECKPOINTING_INTERVAL_MS = 5000;
  private static final String JOB_NAME = "Coinbase ETH-USD ticker";

  private StreamFeatureGroup streamFeatureGroup;

  public EthUsd(Map<String, String> configuration) throws Exception {
    HopsworksConnection hopsworksConnection = HopsworksConnection.builder()
        .host(configuration.get(ConfigKeys.HOST))
        .project(configuration.get(ConfigKeys.PROJECT))
        .apiKeyValue(configuration.get(ConfigKeys.API_KEY_VALUE))
        .build();

    FeatureStore featureStore = hopsworksConnection.getFeatureStore();
    streamFeatureGroup = featureStore.getStreamFeatureGroup(
        configuration.get(ConfigKeys.FEATURE_GROUP_NAME),
        Integer.valueOf(configuration.get(ConfigKeys.FEATURE_GROUP_VERSION))
    );
  }

  public void stream() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStreamSource<Ticker> websocketStream = env.addSource(new WSReader());

    streamFeatureGroup.insertStream(websocketStream);

    env.execute(JOB_NAME);
    env.enableCheckpointing(CHECKPOINTING_INTERVAL_MS);
    env.setRestartStrategy(RestartStrategies.noRestart());
  }
}
