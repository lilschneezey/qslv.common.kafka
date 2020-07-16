package qslv.common.kafka;

import java.time.LocalDateTime;


public class TraceableMessage<T> {
	private String producerAit;
	private String businessTaxonomyId;
	private String correlationId;
	private LocalDateTime messageCreationTime = LocalDateTime.now();
	private LocalDateTime messageCompletionTime = null;
	private T payload;
	
	public TraceableMessage() {
	}
	public TraceableMessage(TraceableMessage<?> clone, T payload) {
		this.producerAit = clone.producerAit;
		this.businessTaxonomyId = clone.businessTaxonomyId;
		this.correlationId = clone.correlationId;
		this.messageCreationTime = clone.messageCreationTime;
		this.payload = payload;
	}
	public String getProducerAit() {
		return producerAit;
	}
	public void setProducerAit(String producerAit) {
		this.producerAit = producerAit;
	}
	public String getBusinessTaxonomyId() {
		return businessTaxonomyId;
	}
	public void setBusinessTaxonomyId(String businessTaxonomyId) {
		this.businessTaxonomyId = businessTaxonomyId;
	}
	public String getCorrelationId() {
		return correlationId;
	}
	public void setCorrelationId(String correlationId) {
		this.correlationId = correlationId;
	}
	public LocalDateTime getMessageCreationTime() {
		return messageCreationTime;
	}
	public LocalDateTime getMessageCompletionTime() {
		return messageCompletionTime;
	}
	public void setMessageCompletionTime(LocalDateTime messageCompletionTime) {
		this.messageCompletionTime = messageCompletionTime;
	}
	public void setMessageCreationTime(LocalDateTime messageCreationTime) {
		this.messageCreationTime = messageCreationTime;
	}
	public T getPayload() {
		return payload;
	}
	public void setPayload(T payload) {
		this.payload = payload;
	}

}
