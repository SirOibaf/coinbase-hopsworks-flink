package ai.hopsworks.coinbaseflink.features;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class PriceWindow extends ProcessWindowFunction<Price5Minutes, Price5Minutes, String, TimeWindow>{
  @Override
  public void process(String s,
                      ProcessWindowFunction<Price5Minutes, Price5Minutes, String, TimeWindow>.Context context,
                      Iterable<Price5Minutes> iterable, Collector<Price5Minutes> collector) {

    Price5Minutes input = iterable.iterator().next();
    input.setTimestamp(context.window().getEnd() * 1000);

    collector.collect(input);
  }
}
