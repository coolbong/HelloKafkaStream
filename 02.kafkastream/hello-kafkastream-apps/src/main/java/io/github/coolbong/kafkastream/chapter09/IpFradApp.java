package io.github.coolbong.kafkastream.chapter09;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;


// Building Data Streaming Applications with Apache Kafka
//
public class IpFradApp {

    private static final String INPUT_TOPIC = "iplog-input";
    private static final String OUTPUT_TOPIC = "iplog-output";

    private static final Set<String> fraudIPList = new HashSet<>();



    public static void main(String[] args){
        preload();

        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "ipfrad-example");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> ipLogs = builder.stream(INPUT_TOPIC);

        final KStream<String, String> fraudIpLogs = ipLogs
                .filter((k, v) -> isFraud(v));
        fraudIpLogs.to(OUTPUT_TOPIC);

        final KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static boolean isFraud(String line) {
        String ip = line.split(" ")[0];
        String[] ranges = ip.split("\\.");

        String range;
        try {
            range = ranges[0];
        } catch (ArrayIndexOutOfBoundsException aioe) {
            range = null;
        }
        return fraudIPList.contains(range);
    }

    private static void preload() {
        fraudIPList.add("212");
        fraudIPList.add("163");
        fraudIPList.add("224");
        fraudIPList.add("126");
        fraudIPList.add("92");
        fraudIPList.add("91");
        fraudIPList.add("10");
        fraudIPList.add("112");
        fraudIPList.add("194");
        fraudIPList.add("198");
        fraudIPList.add("11");
        fraudIPList.add("12");
        fraudIPList.add("13");
        fraudIPList.add("14");
        fraudIPList.add("15");
        fraudIPList.add("16");
    }

}
