package com.reactivetechnologies.mq.common;

import org.springframework.dao.DataAccessException;

public class BlazeInternalError extends DataAccessException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public BlazeInternalError(String msg, Throwable cause) {
		super(msg, cause);
	}

}
