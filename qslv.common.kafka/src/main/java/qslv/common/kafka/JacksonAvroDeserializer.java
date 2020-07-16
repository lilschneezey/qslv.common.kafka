package qslv.common.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroParser;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

/**
 * Deserializes pojo objects from Kafka using the fasterxml Jackson library with Avro databinding.
 * Negotiates with the Confluent schema registry to retrieve avro schemas. Use of Jackson Mixin classes is
 * supported through provided configuration.  See JacksonAvroSerdeConfig. 
 * 
 * Usage: In order to properly configure Confluent Schema Registry, and mixins, the configuration
 * method must first be called before use. Pass the deserializer into the kafka producer factory to use.
 *  
 * Ex: JacksonAvroDeserializer<MyClass> jad = new JacksonAvroDeserializer<>();
 *     jad.configure(props, false);
 *     DefaultKafkaConsumerFactory<String, MyClass> factory = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), jad);
 *     
 * @author SMS
 *
 * @param <T>The class to deserialize
 */
public class JacksonAvroDeserializer<T> extends AbstractKafkaAvroDeserializer implements Deserializer<T> {
	private static final Logger log = LoggerFactory.getLogger(JacksonAvroDeserializer.class);
	
	private final AvroFactory factory = new AvroFactory();
	private AvroMapper mapper = new AvroMapper(new AvroFactory());
	{
		mapper.registerModule(new JavaTimeModule());
		mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
	}
	
	// internal caches
	private Map<Integer,ObjectReader> schemaIdReaderMap = new HashMap<>();
	//private Map<Integer, Class<T>> schemaIdTypeMap = new HashMap<>();
	Class <T> classType = null;

	/**
	 * provided to provide isKey attribute. This is needed for schema subject lookup
	 */
	public void configure(Map<String, ?> config) {
		super.configure(new KafkaAvroDeserializerConfig(config));
		JacksonAvroSerdeConfig.configureMixins(config, mapper);
	}
	public void configure(Map<String, ?> config, boolean isKey) {
		configure(config);
	}
	
	/**
	 * The superclass AbstractKafkaAvroDeserializer maintains a cache of the schemas, 
	 * looked up in the schemaRegristry which is accessed via REST calls.
	 * @param schemaId
	 * @return
	 */
	private Schema getSchema(int schemaId) {
		log.debug("getSchema for {}", schemaId);
		Schema schema = null;
		try {
			schema = super.schemaRegistry.getById(schemaId);
		} catch (IOException | RestClientException ex) {
			log.error("Error getting schema from Schema Registry. schema id: {}" + schemaId, ex);
			throw new SerializationException("Error getting schema from Schema Registry. schema id: " + schemaId, ex);
		}
		log.debug("Schema retrieved for id {} {}", schemaId, schema.toString());
		return schema;
	}
	/**
	 * Looks up the cached reader for the schema id.  The deserializer could be called
	 * with many schemas on the same topic, slowly versioning up.
	 * @param schemaId
	 * @return The Jackson Avro reader
	 */
	private ObjectReader getReader(int schemaId) {
		return schemaIdReaderMap.computeIfAbsent(schemaId, this::computeReader);
	}
	private ObjectReader computeReader(int schemaId) {
		return mapper.reader(new AvroSchema(getSchema(schemaId)));
	}
	
	/**
	 * Looks up the cached data type for the schema id.  The cache is loaded 
	 * from the data type defined in the schema.  We cache these in a map
	 * in the off chance that a message has a schemaid of another type.
	 * But that would certainly cause a problem. All messages must be of type T.
	 * @param schemaId
	 * @return

	private Class<T> getDataType(int schemaId) {
		return schemaIdTypeMap.computeIfAbsent(schemaId, this::loadDataType);
	}
	*/
	@SuppressWarnings("unchecked")
	private Class<T> loadDataType(int schemaId) {
		log.debug("loadDataType {}", schemaId);
		Class<T> clazz = null;
		try {
			Schema schema = getSchema(schemaId);
			clazz = (Class<T>) Class.forName(SpecificData.getClassName(schema), true, Utils.getContextOrKafkaClassLoader());
			//schemaIdTypeMap.put(schemaId, clazz);
			log.debug("Class Type identified for Schema Id {}, class name {}", schemaId, clazz.getCanonicalName());
		} catch (ClassNotFoundException ex) {
			log.debug("Error loading class for schemaId {}. {}", schemaId, ex);
			throw new SerializationException("Error loading class for schema id " + schemaId, ex);
		}
		return clazz;
	}

	/**
	 * Jackson deserializes into an Avro parsed object, then reconstructs the class.
	 */
	@Override
	public T deserialize(String topic, byte[] data) {
		log.debug("deserialize for topic {} byte stream", topic);
		if (data == null) {
			return null;
		}
		T object = null;
		int schemaId = -1;
		try {
			ByteBuffer buffer = ByteBuffer.wrap(data);
			byte magicByte = buffer.get();
			if (AbstractKafkaAvroDeserializer.MAGIC_BYTE != magicByte) {
				log.error( "Expected magic byte not found. Topic {}", topic);
				throw new SerializationException("Expected magic byte not found. Topic " + topic);
			}
			schemaId = buffer.getInt();
			//Class<T> readType = getDataType(schemaId);
			if (classType == null) {
				classType = loadDataType(schemaId);
			}
			
			int length = buffer.limit() -1 - AbstractKafkaAvroDeserializer.idSize;
			int start = buffer.position() + buffer.arrayOffset();
			AvroParser parser = factory.createParser( buffer.array(), start, length);
			Object readObject = getReader(schemaId).readValue(parser, classType);
			object = classType.cast(readObject);
			
		} catch (SerializationException ex) {
			log.error("Error deserializeing for topic {} and schema id {}", topic, schemaId);
			throw ex;
		} catch (IOException ex) {
			log.error("IO Exception deserializing for topic {} and schema id {}", topic, schemaId);
			throw new SerializationException("IO Exception deserializing for topic " + topic + " and schema id " + schemaId, ex);
		}
		return object;
	}
}
