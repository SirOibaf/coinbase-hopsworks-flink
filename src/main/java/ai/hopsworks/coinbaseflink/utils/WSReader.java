package ai.hopsworks.coinbaseflink.utils;

import ai.hopsworks.coinbaseflink.features.Ticker;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class WSReader extends RichSourceFunction<Ticker> {

  private static Logger LOGGER = LoggerFactory.getLogger(WSReader.class);

  private boolean running = true;
  private AsyncHttpClient client;
  private BoundRequestBuilder boundRequestBuilder;
  private WebSocketUpgradeHandler.Builder webSocketListener;
  private BlockingQueue<Ticker> messages = new ArrayBlockingQueue<>(100);

  private ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void run(SourceFunction.SourceContext<Ticker> ctx) throws Exception {
    WebSocketUpgradeHandler webSocketUpgradeHandler = webSocketListener.addWebSocketListener(
        new WebSocketListener() {

          @Override
          public void onOpen(WebSocket webSocket) {
            LOGGER.info("Sending subscribe message");
            webSocket.sendTextFrame("{\n" +
                "    \"type\": \"subscribe\",\n" +
                "    \"product_ids\": [\n" +
                "        \"ETH-USD\"\n" +
                "    ],\n" +
                "    \"channels\": [\"ticker\"]\n" +
                "}");
          }

          @Override
          public void onClose(WebSocket webSocket, int i, String s) {
          }

          @Override
          public void onError(Throwable throwable) {
          }

          @Override
          public void onTextFrame(String payload, boolean finalFragment, int rsv) {
            if (payload != null) {
              LOGGER.info("Payload: " + payload);

              // TODO: find a better way of deserializing the different messages
              Map responseMap = null;
              try {
                responseMap = objectMapper.readValue(payload, Map.class);
              }  catch (IOException e) {
                LOGGER.error("Could not deserialize payload", e);
                return;
              }

              // we only care about the ticker response message
              if (!((String)responseMap.get("type")).equalsIgnoreCase("ticker")) {
                return;
              }

              Instant instant = Instant.parse((String) responseMap.get("time"));

              Ticker ticker = new Ticker("eth-usd",
                  Timestamp.from(instant),
                  Float.valueOf((String) responseMap.get("price")));

              try {
                messages.put(ticker);
              } catch (InterruptedException e) {
                LOGGER.error("Interrupted!", e);
                Thread.currentThread().interrupt();
              }
            }
          }
        }).build();
    boundRequestBuilder.execute(webSocketUpgradeHandler).get();

    while (running) {
      ctx.collect(messages.take());
    }
    running = false;
  }

  @Override
  public void cancel() {
    LOGGER.info("cancel function called");
    running = false;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    LOGGER.info("Configuring the Websocket");
    super.open(parameters);
    configureClient();
  }

  @VisibleForTesting
  public void configureClient() {
    client = Dsl.asyncHttpClient();
    boundRequestBuilder = client.prepareGet("wss://ws-feed.exchange.coinbase.com");
    webSocketListener = new WebSocketUpgradeHandler.Builder();
  }
}
