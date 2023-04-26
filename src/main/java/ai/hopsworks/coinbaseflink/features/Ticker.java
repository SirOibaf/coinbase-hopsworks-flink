package ai.hopsworks.coinbaseflink.features;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Objects;

public class Ticker implements Serializable {

  private String ticker;
  private Timestamp timestamp;
  private Float price;

  public Ticker() {
  }

  public Ticker(String ticker, Timestamp timestamp, Float price) {
    this.ticker = ticker;
    this.timestamp = timestamp;
    this.price = price;
  }

  public String getTicker() {
    return ticker;
  }

  public Float getPrice() {
    return price;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public void setTicker(String ticker) {
    this.ticker = ticker;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  public void setPrice(Float price) {
    this.price = price;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Ticker ticker1 = (Ticker) o;

    if (!Objects.equals(ticker, ticker1.ticker)) return false;
    if (!Objects.equals(timestamp, ticker1.timestamp)) return false;
    return Objects.equals(price, ticker1.price);
  }

  @Override
  public int hashCode() {
    int result = ticker != null ? ticker.hashCode() : 0;
    result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
    result = 31 * result + (price != null ? price.hashCode() : 0);
    return result;
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
