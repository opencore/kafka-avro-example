package com.opencore.sapwebinarseries;


import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Hello world!
 */
public class App {
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("zookeeper.connect", "10.10.0.53:2181");
    props.put("group.id", "avro2json");
    props.put("schema.registry.url", "http://10.10.0.53:8081");

    String topic = "twitter_ingest";
    Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(topic, new Integer(1));

    VerifiableProperties vProps = new VerifiableProperties(props);
    KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(vProps);
    KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);

    ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

    Map<String, List<KafkaStream<Object, Object>>> consumerMap = consumer.createMessageStreams(
        topicCountMap, keyDecoder, valueDecoder);
    KafkaStream stream = consumerMap.get(topic).get(0);
    ConsumerIterator it = stream.iterator();

    while (it.hasNext()) {
      MessageAndMetadata messageAndMetadata = it.next();
      String key = (String) messageAndMetadata.key();
      IndexedRecord value = (IndexedRecord) messageAndMetadata.message();
      List<Schema.Field> fields = value.getSchema().getFields();
      StringBuilder currentLine = new StringBuilder();
      for (Schema.Field field : fields) {
        currentLine.append(field.name());
        currentLine.append(": ");
        currentLine.append(value.get(field.pos()));
        currentLine.append(", ");
      }
      System.out.println(currentLine.toString());
    }
  }
}