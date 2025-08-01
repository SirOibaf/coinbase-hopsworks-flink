package ai.hopsworks.coinbaseflink.features;

import ai.hopsworks.coinbaseflink.utils.Ticker;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.Collections;

public class PriceAccumulator implements AggregateFunction<Ticker, Tuple5<String, Long, Float, Float, Float>, PriceAgg> {

  /*
   * Tuple composition is the following:
   * - String: Ticker
   * - Number of events processed
   * - Price accumulator
   * - Max Price
   * - Min price
   */

  @Override
  public Tuple5<String, Long, Float, Float, Float> createAccumulator() {
    return new Tuple5<>("", 0L, 0.0f, Float.MIN_VALUE, Float.MAX_VALUE);
  }

  @Override
  public Tuple5<String, Long, Float, Float, Float> add(Ticker ticker,
                                                       Tuple5<String, Long, Float, Float, Float> tuple) {
    return new Tuple5<>(
        ticker.getTicker(),
        tuple.f1 + 1,
        tuple.f2 + ticker.getPrice(),
        tuple.f3 > ticker.getPrice() ? tuple.f3 : ticker.getPrice(),
        tuple.f4 < ticker.getPrice() ? tuple.f4 : ticker.getPrice());
  }

  @Override
  public PriceAgg getResult(Tuple5<String, Long, Float, Float, Float> tuple) {
    return new PriceAgg(
        tuple.f0,
        0L, // the timestamp will be set by the PriceWindow method
        tuple.f2 / tuple.f1,
        tuple.f3,
        tuple.f4,
        Collections.singletonList(new Event("sku-1", 123L))
    );
  }

  @Override
  public Tuple5<String, Long, Float, Float, Float> merge(Tuple5<String, Long, Float, Float, Float> tuple1,
                                                         Tuple5<String, Long, Float, Float, Float> tuple2) {
    return new Tuple5<>(
        tuple1.f0,
        tuple1.f1 + tuple2.f1,
        tuple1.f2 + tuple2.f2,
        tuple1.f3 > tuple2.f3 ? tuple1.f3 : tuple2.f3,
        tuple1.f4 < tuple2.f4 ? tuple1.f4 : tuple2.f4
    );
  }
}
