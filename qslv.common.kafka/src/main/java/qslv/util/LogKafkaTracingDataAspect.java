package qslv.util;

import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

import qslv.common.kafka.TraceableMessage;

@Aspect
@Component
public class LogKafkaTracingDataAspect implements BeanFactoryAware {
	Logger log = null;
	StandardEvaluationContext context;
	private ExpressionParser parser = new SpelExpressionParser();
	private TemplateParserContext tcontext = new TemplateParserContext();
	HashMap<String, String> resolvedLiterals = new HashMap<>();

	@Before("@annotation(logKafkaTracingData)")
	public void logTracingData(JoinPoint joinPoint, LogKafkaTracingData logKafkaTracingData) throws Throwable {
		if ( null == log ) {
			if (logKafkaTracingData.logScope().equals(LogKafkaTracingData.class)) {
				log = LoggerFactory.getLogger(joinPoint.getTarget().getClass());
			} else {
				log = LoggerFactory.getLogger(logKafkaTracingData.logScope());
			}
		}
		String value = resolvedLiterals.computeIfAbsent(logKafkaTracingData.value(), this::resolveLiteral);
		String ait = resolvedLiterals.computeIfAbsent(logKafkaTracingData.ait(), this::resolveLiteral);

		for (Object object : joinPoint.getArgs()) {
			if (ConsumerRecord.class.isAssignableFrom(object.getClass()) ) {
				ConsumerRecord<?,?> record = ConsumerRecord.class.cast(object);
				if ( TraceableMessage.class.isAssignableFrom( record.value().getClass() ) ) {
					TraceableMessage<?> message = TraceableMessage.class.cast(record.value());
					
					log.info("TRACE SERVICE={}, AIT={}, CLIENT-AIT={}, BUSINESS-TAXONOMY={}, CORRELATION-ID={}", 
						value, ait, 
						null == message.getProducerAit() ? "null" : message.getProducerAit(), 
						null == message.getBusinessTaxonomyId() ? "null" : message.getBusinessTaxonomyId(), 
						null == message.getCorrelationId() ? "null" : message.getCorrelationId() );
					break;
				}
			}
 		}
	}

	private String resolveLiteral( String literal ) {
		String resolved;
		try {
			resolved = parser.parseExpression(literal,tcontext).getValue(context, String.class);
		} catch (Throwable thrown ) {
			resolved = literal;
			log.error("Parse error for \"{}\" {}", literal, thrown.getMessage());
		}
		return resolved;
	}
	
	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		context = new StandardEvaluationContext(beanFactory);
		context.setBeanResolver(new BeanFactoryResolver(beanFactory));
	}
}
