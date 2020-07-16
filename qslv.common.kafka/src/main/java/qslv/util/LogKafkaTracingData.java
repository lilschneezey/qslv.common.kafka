package qslv.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface LogKafkaTracingData {
	public String value();
	public String ait();
	public Class<?> logScope() default LogKafkaTracingData.class;
}
