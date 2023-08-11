// package io.confluent.kafka.serializers.protobuf;
package io.confluent.kafka.serializers.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class KafkaProtobufSerializer<T extends Message>
    extends AbstractKafkaProtobufSerializer<T> implements Serializer<T> {

  private static int DEFAULT_CACHE_CAPACITY = 1000;

  private boolean isKey;
  private Map<Descriptor, ProtobufSchema> schemaCache;

  /**
   * Constructor used by Kafka producer.
   */
  public KafkaProtobufSerializer() {
    schemaCache = new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
    System.out.println("KafkaProtobufSerializer()");
  }

  public KafkaProtobufSerializer(SchemaRegistryClient client) {
    schemaRegistry = client;
    schemaCache = new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
    System.out.println("KafkaProtobufSerializer(SchemaRegistryClient client)");
  }

  public KafkaProtobufSerializer(SchemaRegistryClient client, Map<String, ?> props) {
    this(client, props, DEFAULT_CACHE_CAPACITY);
    System.out.println("KafkaProtobufSerializer(SchemaRegistryClient client, Map<String, ?> props)");
  }

  public KafkaProtobufSerializer(SchemaRegistryClient client, Map<String, ?> props, int cacheCapacity) {
    schemaRegistry = client;
    configure(serializerConfig(props));
    schemaCache = new BoundedConcurrentHashMap<>(cacheCapacity);
    System.out.println("KafkaProtobufSerializer(SchemaRegistryClient client, Map<String, ?> props, int cacheCapacity)");
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    configure(new KafkaProtobufSerializerConfig(configs));
    System.out.println("void configure(Map<String, ?> configs, boolean isKey)");
  }

  @Override
  public byte[] serialize(String topic, T record) {
    System.out.println("byte[] serialize(String topic, T record)");
    if (schemaRegistry == null) {
      throw new InvalidConfigurationException(
          "SchemaRegistryClient not found. You need to configure the serializer "
              + "or use serializer constructor with SchemaRegistryClient.");
    }
    if (record == null) {
      return null;
    }
    ProtobufSchema schema = schemaCache.get(record.getDescriptorForType());
    if (schema == null) {
      schema = ProtobufSchemaUtils.getSchema(record);
      try {
        // Ensure dependencies are resolved before caching
        boolean autoRegisterForDeps = autoRegisterSchema && !onlyLookupReferencesBySchema;
        boolean useLatestForDeps = useLatestVersion && !onlyLookupReferencesBySchema;
        schema = resolveDependencies(schemaRegistry, normalizeSchema, autoRegisterForDeps,
            useLatestForDeps, latestCompatStrict, latestVersions,
            skipKnownTypes, referenceSubjectNameStrategy, topic, isKey, schema);
      } catch (IOException | RestClientException e) {
        throw new SerializationException("Error serializing Protobuf message", e);
      }
      schemaCache.put(record.getDescriptorForType(), schema);
    }
    return serializeImpl(getSubjectName(topic, isKey, record, schema),
        topic, isKey, record, schema);
  }

  @Override
  public void close() {
  }
}
