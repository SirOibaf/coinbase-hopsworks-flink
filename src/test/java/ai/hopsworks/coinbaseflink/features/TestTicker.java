package ai.hopsworks.coinbaseflink.features;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.avro.reflect.ReflectData;
import org.junit.Test;

import java.util.Collections;

public class TestTicker {

  private String fgSchema = "{\n" +
      "  \"type\" : \"record\",\n" +
      "  \"name\" : \"ticker_1\",\n" +
      "  \"namespace\" : \"mischievous_featurestore.db\",\n" +
      "  \"fields\" : [ {\n" +
      "    \"name\" : \"ticker\",\n" +
      "    \"type\" : [ \"null\", \"string\" ]\n" +
      "  }, {\n" +
      "    \"name\" : \"timestamp\",\n" +
      "    \"type\" : [ \"null\", {\n" +
      "      \"type\" : \"long\",\n" +
      "      \"logicalType\" : \"timestamp-micros\"\n" +
      "    } ]\n" +
      "  }, {\n" +
      "    \"name\" : \"price\",\n" +
      "    \"type\" : [ \"null\", \"float\" ]\n" +
      "  } ]\n" +
      "}";

  @Test
  public void TestCanSerialize() throws Exception {
    Schema avroSchema = new Schema.Parser().parse(fgSchema);

    Schema pojoSchema = ReflectData.get().getSchema(Ticker.class);
    new SchemaValidatorBuilder()
        .canReadStrategy()
        .validateAll()
        .validate(avroSchema, Collections.singletonList(pojoSchema));
  }

}
