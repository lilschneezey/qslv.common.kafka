package qslv.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import qslv.common.kafka.TraceableMessage;

@Aspect
@Component
public class LogKafkaTracingDataAspect {
	Logger log = null;

	@Before("@annotation(logKafkaTracingData)")
	public void logTracingData(JoinPoint joinPoint, LogKafkaTracingData logKafkaTracingData) throws Throwable {
		if ( null == log ) {
			if (logKafkaTracingData.logScope().equals(LogKafkaTracingData.class)) {
				log = LoggerFactory.getLogger(joinPoint.getTarget().getClass());
			} else {
				log = LoggerFactory.getLogger(logKafkaTracingData.logScope());
			}
		}
		for (Object object : joinPoint.getArgs()) {
			if (ConsumerRecord.class.isAssignableFrom(object.getClass()) ) {
				ConsumerRecord<?,?> record = ConsumerRecord.class.cast(object);
				if ( TraceableMessage.class.isAssignableFrom( record.value().getClass() ) ) {
					TraceableMessage<?> message = TraceableMessage.class.cast(record.value());
					
					log.info("TRACE SERVICE={}, AIT={}, CLIENT-AIT={}, BUSINESS-TAXONOMY={}, CORRELATION-ID={}", 
						logKafkaTracingData.value(), logKafkaTracingData.ait(), 
						null == message.getProducerAit() ? "null" : message.getProducerAit(), 
						null == message.getBusinessTaxonomyId() ? "null" : message.getBusinessTaxonomyId(), 
						null == message.getCorrelationId() ? "null" : message.getCorrelationId() );
					break;
				}
			}
 		}
	}
}
