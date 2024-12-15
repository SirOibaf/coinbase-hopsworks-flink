package ai.hopsworks.coinbaseflink.features;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class PriceWindow extends ProcessWindowFunction<PriceAgg, PriceAgg, String, TimeWindow>{
  @Override
  public void process(String s,
                      ProcessWindowFunction<PriceAgg, PriceAgg, String, TimeWindow>.Context context,
                      Iterable<PriceAgg> iterable, Collector<PriceAgg> collector) {

    PriceAgg input = iterable.iterator().next();
    input.setTimestamp(context.window().getEnd() * 1000);

    collector.collect(input);
  }
}
