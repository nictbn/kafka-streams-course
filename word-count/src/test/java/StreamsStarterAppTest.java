import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class StreamsStarterAppTest {
    private static final String INPUT_TOPIC = "word-count-input";
    private static final String OUTPUT_TOPIC = "word-count-output";
    TopologyTestDriver testDriver;

    private TestInputTopic<Long, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;


    @BeforeEach
    public void setUpTopologyTestDriver(){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsStarterApp wordCountApp = new StreamsStarterApp();
        Topology topology = wordCountApp.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC, new LongSerializer(), new StringSerializer());
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), new LongDeserializer());
    }

    @AfterEach
    public void closeTestDriver(){
        testDriver.close();
    }

    @Test
    public void makeSureCountsAreCorrect(){
        String firstExample = "testing Kafka Streams";
        pushNewInputRecord(firstExample);
        verifyKeyAndValue(readOutput(), "testing", 1L);
        verifyKeyAndValue(readOutput(), "kafka", 1L);
        verifyKeyAndValue(readOutput(), "streams", 1L);
        assertTrue(outputTopic.isEmpty());

        String secondExample = "testing Kafka again";
        pushNewInputRecord(secondExample);
        verifyKeyAndValue(readOutput(), "testing", 2L);
        verifyKeyAndValue(readOutput(), "kafka", 2L);
        verifyKeyAndValue(readOutput(), "again", 1L);
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void makeSureWordsBecomeLowercase(){
        String upperCaseString = "KAFKA kafka Kafka";
        pushNewInputRecord(upperCaseString);
        verifyKeyAndValue(readOutput(), "kafka", 1L);
        verifyKeyAndValue(readOutput(), "kafka", 2L);
        verifyKeyAndValue(readOutput(), "kafka", 3L);
        assertTrue(outputTopic.isEmpty());
    }

    public void pushNewInputRecord(String value){
        inputTopic.pipeInput(value);
    }

    public TestRecord<String, Long> readOutput(){
        return outputTopic.readRecord();
    }

    private static void verifyKeyAndValue(TestRecord<String, Long> firstInput, String expectedKey, long expectedValue) {
        assertEquals(expectedKey, firstInput.getKey());
        assertEquals(expectedValue, firstInput.getValue());
    }
}