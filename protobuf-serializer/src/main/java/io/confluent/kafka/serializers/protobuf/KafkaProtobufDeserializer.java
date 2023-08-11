// package io.confluent.kafka.serializers.protobuf;
package protobuf;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class KafkaProtobufDeserializer<T extends Message>
    extends AbstractKafkaProtobufDeserializer<T> implements Deserializer<T> {

  /**
   * Constructor used by Kafka consumer.
   */
  public KafkaProtobufDeserializer() {
    System.out.println("public KafkaProtobufDeserializer()");
  }

  public KafkaProtobufDeserializer(SchemaRegistryClient client) {
    schemaRegistry = client;
    System.out.println("public KafkaProtobufDeserializer(SchemaRegistryClient client)");
  }

  public KafkaProtobufDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
    this(client, props, null);
    System.out.println("public KafkaProtobufDeserializer(SchemaRegistryClient client, Map<String, ?> props)");
  }

  @VisibleForTesting
  public KafkaProtobufDeserializer(SchemaRegistryClient client,
                                   Map<String, ?> props,
                                   Class<T> type) {
    schemaRegistry = client;
    configure(deserializerConfig(props), type);
    System.out.println("public KafkaProtobufDeserializer(SchemaRegistryClient client, Map<String, ?> props, Class<T> type)");
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    configure(new KafkaProtobufDeserializerConfig(configs), isKey);
    System.out.println("public void configure(Map<String, ?> configs, boolean isKey) isKey=" + isKey);
    for (Map.Entry<String, ?> entry: configs.entrySet()) {
      System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
    }
  }

  @SuppressWarnings("unchecked")
  protected void configure(KafkaProtobufDeserializerConfig config, boolean isKey) {
    this.isKey = isKey;
    if (isKey) {
      configure(
          config,
          (Class<T>) config.getClass(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_KEY_TYPE)
      );
    } else {
      configure(
          config,
          (Class<T>) config.getClass(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE)
      );
    }
    System.out.println("public void configure(KafkaProtobufDeserializerConfig config, boolean isKey)");
    for (Map.Entry<String, ?> entry: config.originalsStrings().entrySet()) {
      System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
    }
  }

  @Override
  public T deserialize(String topic, byte[] bytes) {
    System.out.println("public T deserialize(String topic, byte[] bytes)");
    return (T) deserialize(false, topic, isKey, bytes);
  }

  @Override
  public void close() {

  }
}
