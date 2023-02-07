package com.davidlucena.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;

public class WoldCountApp {
  public static void main(String[] args) {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();

    // 1. Stream from kafka
    KStream<String, String> wordCountInput = builder.stream("word-count-input");

    // 2. Map values to lowercase
    KTable<String, Long> wordCounts = wordCountInput.mapValues(textLine -> textLine.toLowerCase())
      // 3. FlatMap split by space
      .flatMapValues(lowercasedTextLine -> Arrays.asList(lowercasedTextLine.split(" ")))
      // 4. Select key to apply a key (we discard the old key)
      .selectKey((ignoredKey, word) -> word)
      // 5. Group by key before aggregation
      .groupByKey()
      // 6. Count occurrences
      .count(Named.as("Counts"));

    // 7. To in order to write the results back to kafka
    wordCounts.toStream().to("word-count-output");

    KafkaStreams streams = new KafkaStreams(builder.build(), config);
    streams.start();

    // Printing the topology
    System.out.println(streams.toString());

    // Shutdown hook to correctly close the stream application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
