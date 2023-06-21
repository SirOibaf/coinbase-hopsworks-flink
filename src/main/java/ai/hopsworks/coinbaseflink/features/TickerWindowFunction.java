package ai.hopsworks.coinbaseflink.features;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TickerWindowFunction extends
  ProcessWindowFunction<Price5Minutes, Price5Minutes, String,  TimeWindow> {
  
  @Override
  public void process(String key, Context context,
    Iterable<Price5Minutes> iterable, Collector<Price5Minutes> collector) throws Exception {
  
    Price5Minutes aggregationEvent = iterable.iterator().next();
    Price5Minutes output = new Price5Minutes();
  
    output.setTicker(aggregationEvent.getTicker());
    output.setTimestamp(aggregationEvent.getTimestamp());
    output.setPriceAvg(aggregationEvent.getPriceAvg());
    output.setPriceMax(aggregationEvent.getPriceMax());
    output.setPriceMin(aggregationEvent.getPriceMin());
    output.setWindowStart(context.window().getStart());
    output.setWindowEnd(context.window().getEnd());
    
    collector.collect(
      output
    );
  }
}
