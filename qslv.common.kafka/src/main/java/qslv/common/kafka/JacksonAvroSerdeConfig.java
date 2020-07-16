package qslv.common.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * For most cases Jackson will create a good-enough schema for your pojo. With Jackson annotations
 * you can create much customization.  However, Legacy classes that cannot be modified may need further
 * direction for Jackson. In this situation, a Mixin configuration property can be added.  A mixin holds only the
 * Jackson annotations. Jackson will add them in as it de/serializes the object.
 * 
 * Ex: props.put(JacksonAvroSerdeConfig.CONFIG_MIXINS + "qslv.common.kafka.test.TestPojo5", "qslv.common.kafka.test.TestPojo5Mixin");
 * 
 * Within the Mixin class, you only define the attributes to which you want to add Json annotations. Declare
 * the attribute with the same scope and type along with the Json annoation. Only a few annotations are useful
 * within the fasterxml avro library.
 * 
 * Ex:  @JsonFormat(shape=Shape.String, pattern="yyyyMMdd") private Date startDate;
 * Ex:  @JsonIgnore private LegacyClass legacy;
 * 
 * In addition, if you wish to avoid Jackson's creation of the schema. There are two ways to supply a custom avsc schema file.
 * The first is to supply the schema file with the same classpath and name as the class. 
 * 
 * Ex: qslv.common.kafka.Pojo5 and qslv.common.kafka.Pojo5.avsc
 * 
 * The second is to specify a configuration property with the location of the avsc file.
 * 
 * Ex: props.put(JacksonAvroSerdeConfig.CONFIG_SCHEMA + "qslv.common.kafka.Pojo5", "\\qslv\\common\\kafka\\MyCustomPojoFile.avsc");
 * 
 * @author SMS
 *
 */
public class JacksonAvroSerdeConfig {
	private static final Logger log = LoggerFactory.getLogger(JacksonAvroSerdeConfig.class);

	public static final String CONFIG_MIXINS = "mapper.mixins.";
	public static final String CONFIG_SCHEMA = "mapper.schema.";
	
	public static Map<String, Schema>  configureSchemas(Map<String,?> props) {
		Map<String, Schema> configuredSchemaMap  = new HashMap<String, Schema>();
		props.forEach((key,value) -> {
			try {
				if (key.startsWith(CONFIG_SCHEMA)) {
					String classname = key.substring(CONFIG_SCHEMA.length()).trim();
					if ( false == configuredSchemaMap.containsKey(classname)) {
						Schema schema = loadSchema(value.toString().trim());
						if ( schema == null ) {
							log.error("Config: schema file missing. Class " + classname + " schema file " + value.toString().trim());
						} else {
							configuredSchemaMap.put(classname, schema);
							log.debug("Config: Class " + classname + " with schema file " + value.toString().trim());
						}
					}
				}
			} catch ( IOException ex) {
				log.error(ex.getLocalizedMessage());
				throw new SerializationException("Configured schema not found.", ex);
			}
		});
		return configuredSchemaMap;
	}
	
	public static void  configureMixins(Map<String,?> props, ObjectMapper mapper) {
		props.forEach((key,value) -> {
			try {
				if (key.startsWith(CONFIG_MIXINS)) {
					Class<?> clazz = Class.forName(key.substring(CONFIG_MIXINS.length()).trim());
					Class<?> mixinClazz = Class.forName(value.toString().trim());
					mapper.addMixIn(clazz,mixinClazz);
					log.debug("Config: Class " + clazz.getCanonicalName() + " with mixin class "+ mixinClazz.getCanonicalName());
				}
			} catch ( ClassNotFoundException ex) {
				log.error(ex.getLocalizedMessage());
				throw new SerializationException("Mixin class not found.", ex);
			}
		});
		return ;
	}
	public static Schema loadSchema(String resourceName) throws IOException {
		ClassPathResource classPathResource = new ClassPathResource(resourceName);
		if (classPathResource.exists()) {
			InputStream inputStream = classPathResource.getInputStream();
			return new Schema.Parser().parse(inputStream); 
		}
		return null;
	}
}
