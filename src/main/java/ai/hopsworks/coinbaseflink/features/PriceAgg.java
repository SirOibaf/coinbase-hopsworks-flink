package ai.hopsworks.coinbaseflink.features;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@AllArgsConstructor
public class PriceAgg {

  @Getter
  @Setter
  private String ticker;

  @Getter
  @Setter
  private Long timestamp;

  @Getter
  @Setter
  private Float price_avg;

  @Getter
  @Setter
  private Float price_max;

  @Getter
  @Setter
  private Float price_min;

  @Getter
  @Setter
  private List<Event> events;
}
