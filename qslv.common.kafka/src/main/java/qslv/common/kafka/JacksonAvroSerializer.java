package qslv.common.kafka;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

/**
 * Serializes pojo objects to Kafka using the fasterxml Jackson library with Avro databinding.
 * Negotiates with the Confluent schema registry to register new schemas using Jackson, or a provided schema file.
 * See JacksonAvroSerdeConfig for details.
 * 
 * Usage: In order to properly configure Confluent Schema Registry, mixins, and avro schema files, the
 * configuration method must first be called before use. Pass the serializer into the kafka producer factory to use.
 *  
 * Ex: JacksonAvroSerializer<MyClass> jas = new JacksonAvroSerializer<>();
 *     jas.configure(props, false);
 *     new DefaultKafkaProducerFactory<String, MyClass>(props, new StringSerializer(), jas);
 *     
 * @author SMS
 *
 * @param <T>The class to serialize
 */
public class JacksonAvroSerializer<T> extends AbstractKafkaAvroSerializer implements Serializer<T> {
	private static final Logger log = LoggerFactory.getLogger(JacksonAvroSerializer.class);
	
	private static String AVRO_FILE_EXTENSION = ".avsc";

	private static AvroFactory avroFactory = new AvroFactory();
	private AvroMapper mapper = new AvroMapper(avroFactory);
	private boolean isKey = false;
	private JavaType type = null;
	
	// internal caches
	private Map<Integer,Schema> schemaIdSchemaMap = new HashMap<>();
	private Map<String,Integer> topicSchemaIdMap = new HashMap<>();
	private Map<Integer, ObjectWriter> schemaIdWriterMap = new HashMap<>();
	private Map<String, Schema> configuredSchemaMap = null;

	{
		mapper.registerModule(new JavaTimeModule());
		mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
	}
	
	/**
	 * provided to provide isKey attribute. This is needed for schema subject lookup
	 */
	public void configure(Map<String, ?> config, boolean isKey) {
		log.debug("config {} {}", config, isKey);
		this.isKey = isKey;
		super.configure(new KafkaAvroSerializerConfig(config));
		configuredSchemaMap = JacksonAvroSerdeConfig.configureSchemas(config);
		JacksonAvroSerdeConfig.configureMixins(config, mapper);
	}
	public void configure(Map<String, ?> config, boolean isKey, JavaType type) {
		this.type = type;
		configure(config, isKey);
	}
	public TypeFactory getTypeFactory() {
		return mapper.getTypeFactory();
	}

	public byte[] serialize(String topic, T object) {
		log.debug("serialize {} {}", topic, object);
		if (object == null) {
			return null;
		}

		int schemaId = getSchemaId(topic, object);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		BinaryEncoder be = EncoderFactory.get().directBinaryEncoder(bos, null);
		try {
			bos.write(AbstractKafkaAvroSerializer.MAGIC_BYTE);
			bos.write(ByteBuffer.allocate(AbstractKafkaAvroSerializer.idSize).putInt(schemaId).array());
			getWriter(schemaId).writeValue(bos, object);
			be.flush();
			bos.close();
		} catch (SerializationException ex) {
			log.debug(ex.getLocalizedMessage());
			throw ex;
		} catch (Exception ex) {
			log.error("Error Serializing " + object.getClass() + ex);
			throw new SerializationException("Error Serializing " + object.getClass(), ex);
		}
		return bos.toByteArray();
		//TODO: how to measure communication to kafka?
	}

	private ObjectWriter getWriter(int schemaId) {
		return schemaIdWriterMap.computeIfAbsent(schemaId, this::computeWriter);
	}
	private ObjectWriter computeWriter(int schemaId) {
		return mapper.writer(new AvroSchema(schemaIdSchemaMap.get(schemaId)));
	}

	private int getSchemaId(String topic, T object) {
		return topicSchemaIdMap.computeIfAbsent(topic, k->calculateSchemaId(topic,object));
	}

	/**
	 * For a new topic encountered, register or retrieve the schema and return the
	 * associated schema Id.
	 * @param topic
	 * @param obj
	 * @return
	 */
	private int calculateSchemaId(String topic, T obj) {
		log.debug("calculateSchemaId {} {}", topic, obj.getClass().getCanonicalName());
		Schema schema = loadSchema(topic, obj);

		String subject = super.getSubjectName(topic, this.isKey, null, schema );
		int schemaId = 0;
		try {
			// TODO: add monitor timer and log
			if (super.autoRegisterSchema) {
				schemaId = super.schemaRegistry.register(subject, schema);
			} else {
				schemaId = super.schemaRegistry.getId(subject, schema);
			}
		} catch (Exception ex) {
			log.warn("Error registering or retrieving Avro Schema: " + subject, ex);
			throw new SerializationException("Error retrieving Avro schema" + subject, ex);
		}
		log.debug("Schema Id {} registered for subject {}", schemaId, subject);
		log.debug(schema.toString());
	
		schemaIdSchemaMap.putIfAbsent(schemaId, schema);
		return schemaId;
	}
	/**
	 * Loads a schema from one of three sources: 1) configured, 2) an avsc file that matches
	 * the class name, and 3) Jackson will build it. The schema is saved in a mapped cache.
	 * @param topic
	 * @param object
	 * @return
	 */
	private Schema loadSchema(String topic, T object) {
		log.debug("loadSchema {} {}", topic, object.getClass().getCanonicalName());
		// first try the list of configured schemas
		Schema schema = configuredSchemaMap.get(object.getClass().getCanonicalName());
		if (schema != null ) {
			log.debug("Using configured schema for topic {} for class {}", topic, object.getClass().getCanonicalName());
			return schema;
		}
		
		// second lookup the schema file, using the class name with an .avsc extension
		String filename = new String(object.getClass().getCanonicalName()).replace('.',File.separatorChar).concat(AVRO_FILE_EXTENSION);
		try {
			schema = JacksonAvroSerdeConfig.loadSchema( filename );
		} catch (IOException ex) {
			log.error("Error reading matching .avsc file for {} for class {}", topic, object.getClass().getCanonicalName());
			throw new SerializationException("Could not read matching .avsc file for " + topic + " for class " 
					+ object.getClass().getCanonicalName(), ex);
		}
		if ( schema != null ) {
			log.debug("Using matching .avsc file for topic {} for class {}", topic, object.getClass().getCanonicalName());
			return schema;
		}
		
		// finally, build the schema using Jackson. Mixins should have already been configured.
		try {
			if (this.type == null ) {
				schema = this.mapper.schemaFor(object.getClass()).getAvroSchema();
			} else {
				schema = this.mapper.schemaFor(this.type).getAvroSchema();
			}
		} catch (JsonMappingException ex) {
			log.error("Could not generate schema for topic {} class {}", topic, object.getClass().getCanonicalName());
			throw new SerializationException("Could not generate schema for topic " + topic + " for class " 
					+ object.getClass().getCanonicalName(), ex);
		}
		log.debug("Using Jackson generated schema for topic {} for class {}", topic, object.getClass().getCanonicalName());
		return schema;
	}
}

