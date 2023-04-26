package ai.hopsworks.coinbaseflink.utils;

import org.junit.Test;

public class TestWSReader {

  @Test
  public void TestTicketSubscription() throws Exception {
    WSReader wsReader = new WSReader();
    wsReader.configureClient();

    wsReader.run(null);
  }

}
