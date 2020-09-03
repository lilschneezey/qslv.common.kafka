package qslv.common.kafka;

import java.time.LocalDateTime;

public class ResponseMessage<T, R> {
	final public static int SUCCESS = 0;
	final public static int INSUFFICIENT_FUNDS = 1;
	final public static int MALFORMED_MESSAGE = 2;
	final public static int CONFLICT = 3;
	final public static int INTERNAL_ERROR = 4;
	
	private int status = SUCCESS;
	private String errorMessage=null;
	private R response = null;
	private T request = null;
	private String producerAit;
	private String responderAit;
	private String businessTaxonomyId;
	private String correlationId;
	private LocalDateTime messageCreationTime = null;
	private LocalDateTime messageCompletionTime = null;
	
	public ResponseMessage() {
	}
	public ResponseMessage(T request) {
		this.request = request;
	}
	public ResponseMessage(TraceableMessage<?> clone, T request) {
		this.businessTaxonomyId = clone.getBusinessTaxonomyId();
		this.producerAit = clone.getProducerAit();
		this.correlationId = clone.getCorrelationId();
		this.messageCreationTime = clone.getMessageCreationTime();
		this.messageCompletionTime = clone.getMessageCompletionTime();
		this.request = request;
	}
	public ResponseMessage(T request, R response) {
		this.request = request;
		this.response = response;
	}
	public ResponseMessage(TraceableMessage<?> clone, T request, R response) {
		this.businessTaxonomyId = clone.getBusinessTaxonomyId();
		this.producerAit = clone.getProducerAit();
		this.correlationId = clone.getCorrelationId();
		this.messageCreationTime = clone.getMessageCreationTime();
		this.messageCompletionTime = clone.getMessageCompletionTime();
		this.request = request;
		this.response = response;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public String getErrorMessage() {
		return errorMessage;
	}
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
	public R getResponse() {
		return response;
	}
	public void setResponse(R response) {
		this.response = response;
	}
	public T getRequest() {
		return request;
	}
	public void setRequest(T request) {
		this.request = request;
	}
	public String getProducerAit() {
		return producerAit;
	}
	public void setProducerAit(String producerAit) {
		this.producerAit = producerAit;
	}
	public String getResponderAit() {
		return responderAit;
	}
	public void setResponderAit(String responderAit) {
		this.responderAit = responderAit;
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
	public void setMessageCreationTime(LocalDateTime messageCreationTime) {
		this.messageCreationTime = messageCreationTime;
	}
	public LocalDateTime getMessageCompletionTime() {
		return messageCompletionTime;
	}
	public void setMessageCompletionTime(LocalDateTime messageCompletionTime) {
		this.messageCompletionTime = messageCompletionTime;
	}
	
}
