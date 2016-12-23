package com.blaze.mq.consume;

import com.blaze.mq.Data;

public interface Consumer<T extends Data> {

	/**
	 * Callback method invoked on message added to queue.
	 * 
	 * @param <T>
	 * @param m
	 * @throws Exception
	 */
	void onMessage(T m) throws Exception;

}