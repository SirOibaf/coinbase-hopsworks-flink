package ai.hopsworks.coinbaseflink;

import ai.hopsworks.coinbaseflink.features.EthUsd;

import java.util.HashMap;
import java.util.Map;

public class CoinbaseFlink {

  public static void main(String[] args) throws Exception {
    Map<String, String> configuration = parseConfiguration(args);
    new EthUsd(configuration).stream();
  }

  private static Map<String, String> parseConfiguration(String[] args) throws Exception {
    return new HashMap<>();
  }
}
