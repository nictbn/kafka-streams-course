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

    public void pushNewInputRecord(String value){
        inputTopic.pipeInput(value);
    }

    public TestRecord<String, Long> readOutput(){
        return outputTopic.readRecord();
    }

    @Test
    public void makeSureCountsAreCorrect(){
        String firstExample = "testing Kafka Streams";
        pushNewInputRecord(firstExample);
        TestRecord<String, Long> firstInput = readOutput();
        assertEquals("testing", firstInput.getKey());
        assertEquals(1L, firstInput.getValue());

        TestRecord<String, Long> second = readOutput();
        assertEquals("kafka", second.getKey());
        assertEquals(1L, second.getValue());

        TestRecord<String, Long> third = readOutput();
        assertEquals("streams", third.getKey());
        assertEquals(1L, third.getValue());
        assertTrue(outputTopic.isEmpty());

        String secondExample = "testing Kafka again";
        pushNewInputRecord(secondExample);
        TestRecord<String, Long> fourth = readOutput();
        assertEquals("testing", fourth.getKey());
        assertEquals(2L, fourth.getValue());

        TestRecord<String, Long> fifth = readOutput();
        assertEquals("kafka", fifth.getKey());
        assertEquals(2L, fifth.getValue());

        TestRecord<String, Long> sixth = readOutput();
        assertEquals("again", sixth.getKey());
        assertEquals(1L, sixth.getValue());
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void makeSureWordsBecomeLowercase(){
        String upperCaseString = "KAFKA kafka Kafka";
        pushNewInputRecord(upperCaseString);
        TestRecord<String, Long> first = readOutput();
        assertEquals("kafka", first.getKey());
        assertEquals(1L, first.getValue());

        TestRecord<String, Long> second = readOutput();
        assertEquals("kafka", second.getKey());
        assertEquals(2L, second.getValue());

        TestRecord<String, Long> third = readOutput();
        assertEquals("kafka", third.getKey());
        assertEquals(3L, third.getValue());
    }

}