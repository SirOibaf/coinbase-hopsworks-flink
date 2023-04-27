package ai.hopsworks.coinbaseflink.features;

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

public class EthUsd {

  public static final int CHECKPOINTING_INTERVAL_MS = 5000;
  private static final String JOB_NAME = "Coinbase ETH-USD ticker";

  private StreamFeatureGroup streamFeatureGroup;

  public EthUsd() throws Exception {
    // FIXME: why can't I use the builder?
    HopsworksConnection hopsworksConnection = new HopsworksConnection(
        "ef3beed0-e437-11ed-9473-fd53c07a424c.cloud.hopsworks.ai",
        443,
        "mischievous",
        null,
        null,
        false,
        null,
        null,
        null,
        "wJP4J8zVO76abcZs.6SETE5tTgqGsTogRLxKPtfLLU37pn3G5zcqj0HQjDDMrwDfOqe6OK0BQg37aMolV"
    );
    FeatureStore featureStore = hopsworksConnection.getFeatureStore();
    streamFeatureGroup = featureStore.getStreamFeatureGroup("ticker", 2);
  }

  public void stream() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();
    Class<?> unmodColl = Class.forName("java.util.Collections$UnmodifiableCollection");
    env.getConfig().addDefaultKryoSerializer(unmodColl, UnmodifiableCollectionSerializer.class);


    env.setParallelism(1);

    DataStreamSource<Ticker> websocketStream = env.addSource(new WSReader());

    //StreamingFileSink<String> streamingFileSink = StreamingFileSink
    //    .forRowFormat(new Path("hdfs:///Projects/flinke/data/test"),
    //        new SimpleStringEncoder<String>("UTF-8"))
    //     .build();

    //websocketStream.map(Ticker::toString).addSink(streamingFileSink);

     streamFeatureGroup.insertStream(websocketStream);

     env.execute(JOB_NAME);
     env.enableCheckpointing(CHECKPOINTING_INTERVAL_MS);
     env.setRestartStrategy(RestartStrategies.noRestart());
  }
}
