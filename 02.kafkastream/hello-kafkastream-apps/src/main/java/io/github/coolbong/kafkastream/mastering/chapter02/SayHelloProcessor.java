package io.github.coolbong.kafkastream.mastering.chapter02;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

public class SayHelloProcessor implements Processor<Void, String, Void, Void> {
    @Override
    public void process(Record<Void, String> record) {
        System.out.println("Processor) Hello, " + record.value());
    }
}
