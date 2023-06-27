package ai.hopsworks.coinbaseflink.utils;

import java.time.Instant;

public class Ticker {

  private String ticker;
  private Instant timestamp;
  private Float price;

  public Ticker(String ticker, Instant timestamp, Float price) {
    this.ticker = ticker;
    this.timestamp = timestamp;
    this.price = price;
  }

  public String getTicker() {
    return ticker;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public Float getPrice() {
    return price;
  }

  public void setTicker(String ticker) {
    this.ticker = ticker;
  }

  public void setTimestamp(Instant timestamp) {
    this.timestamp = timestamp;
  }

  public void setPrice(Float price) {
    this.price = price;
  }

  @Override
  public String toString() {
    return "Ticker{" +
        "ticker='" + ticker + '\'' +
        ", timestamp=" + timestamp +
        ", price=" + price +
        '}';
  }
}
