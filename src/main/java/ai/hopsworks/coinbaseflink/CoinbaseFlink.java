package ai.hopsworks.coinbaseflink;

import ai.hopsworks.coinbaseflink.features.EthUsd;

public class CoinbaseFlink {

  public static void main(String[] args) throws Exception {
    new EthUsd().stream();
  }

}
