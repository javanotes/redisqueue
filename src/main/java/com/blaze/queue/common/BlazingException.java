package com.blaze.queue.common;

import com.blaze.queue.Data;

public class BlazingException extends Exception {

	private Data record;
	public BlazingException(String msg, Throwable cause) {
		super(msg, cause);
	}
	public BlazingException(Throwable cause) {
		super("", cause);
	}
	public Data getRecord() {
		return record;
	}
	public void setRecord(Data record) {
		this.record = record;
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
