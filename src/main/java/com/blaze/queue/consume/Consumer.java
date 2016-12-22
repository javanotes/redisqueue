package com.blaze.queue.consume;

import com.blaze.queue.Data;

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