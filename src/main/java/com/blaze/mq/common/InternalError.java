package com.blaze.mq.common;

import org.springframework.dao.DataAccessException;

public class InternalError extends DataAccessException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public InternalError(String msg, Throwable cause) {
		super(msg, cause);
	}

}
