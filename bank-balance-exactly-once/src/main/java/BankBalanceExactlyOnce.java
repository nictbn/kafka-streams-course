import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.Properties;

public class BankBalanceExactlyOnce {

    public static final String BANK_TRANSACTIONS_TOPIC = "bank-transactions";
    public static final String COUNT_FIELD = "count";
    public static final String BALANCE_FIELD = "balance";
    public static final String AMOUNT_FIELD = "amount";
    public static final String TIME_FIELD = "time";
    public static final String BANK_BALANCE_AGGREGATION_TOPIC = "bank-balance-agg";
    public static final String BANK_BALANCE_EXACTLY_ONCE_TOPIC = "bank-balance-exactly-once";

    public static void sTart() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JsonNode> bankTransactions = builder.stream(BANK_TRANSACTIONS_TOPIC, Consumed.with(Serdes.String(), jsonSerde));
        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put(COUNT_FIELD, 0);
        initialBalance.put(BALANCE_FIELD, 0);
        initialBalance.put(TIME_FIELD, Instant.ofEpochMilli(0L).toString());

        KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
                .aggregate(
                        () -> initialBalance,
                        (key, transaction, balance) -> newBalance(transaction, balance),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as(BANK_BALANCE_AGGREGATION_TOPIC)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde)
                );
        bankBalance.toStream().to(BANK_BALANCE_EXACTLY_ONCE_TOPIC, Produced.with(Serdes.String(), jsonSerde));
        KafkaStreams stream = new KafkaStreams(builder.build(), config);
        stream.start();
        System.out.println(stream);
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put(COUNT_FIELD, balance.get(COUNT_FIELD).asInt() + 1);
        newBalance.put(BALANCE_FIELD, balance.get(BALANCE_FIELD).asInt() + transaction.get(AMOUNT_FIELD).asInt());
        long balanceEpoch = Instant.parse(balance.get(TIME_FIELD).asText()).toEpochMilli();
        long transactionEpoch = Instant.parse(transaction.get(TIME_FIELD).asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put(TIME_FIELD, newBalanceInstant.toString());
        return newBalance;
    }
}
