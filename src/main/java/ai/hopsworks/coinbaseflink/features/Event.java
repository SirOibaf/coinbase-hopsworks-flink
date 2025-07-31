package ai.hopsworks.coinbaseflink.features;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
public class Event {

  @Getter
  @Setter
  private String sku;

  @Getter
  @Setter
  private Long timestamp;
}
