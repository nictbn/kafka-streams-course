import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColor {

    public static final String USER_KEYS_AND_COLORS_TOPIC = "user-keys-and-colors";
    public static final String FAVOURITE_COLOR_OUTPUT_TOPIC = "favourite-color-output";
    public static final String FAVOURITE_COLOR_INPUT_TOPIC = "favourite-color-input";

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(FAVOURITE_COLOR_INPUT_TOPIC);

        KStream<String, String> usersAndColors = textLines
                .filter((key, value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                .mapValues((value) -> value.split(",")[1].toLowerCase())
                .filter((user, color) -> Arrays.asList("green", "blue", "red").contains(color));
        usersAndColors.to(USER_KEYS_AND_COLORS_TOPIC);
        KTable<String, String> usersAndColorsTable = builder.table(USER_KEYS_AND_COLORS_TOPIC);
        KTable<String, Long> favouriteColors = usersAndColorsTable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count(Named.as("CountsByColors"));
        favouriteColors.toStream().to(FAVOURITE_COLOR_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams stream = new KafkaStreams(builder.build(), config);
        stream.cleanUp();
        stream.start();

        System.out.println(stream);
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }
}
