package qslv.common.kafka;

import java.time.LocalDateTime;

public class ResponseMessage<T, R> {
	public static int SUCCESS=0;
	public static int MALFORMED_MESSAGE=1;
	public static int INTERNAL_ERROR=2;
	
	private int status = SUCCESS;
	private String message=null;
	private R response = null;
	private T request = null;
	private LocalDateTime processStart = null;
	private LocalDateTime processEnd = null;
	
	public ResponseMessage() {
	}
	public ResponseMessage(T request) {
		this.request = request;
	}
	public ResponseMessage(T request, R response) {
		this.request = request;
		this.response = response;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
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
	public LocalDateTime getProcessStart() {
		return processStart;
	}
	public void setProcessStart(LocalDateTime processStart) {
		this.processStart = processStart;
	}
	public LocalDateTime getProcessEnd() {
		return processEnd;
	}
	public void setProcessEnd(LocalDateTime processEnd) {
		this.processEnd = processEnd;
	}
	
}
