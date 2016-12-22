package com.blaze.queue.consume;

import com.blaze.queue.Data;

/**
 * Callback on a CMQ. Do NOT use this interface directly. Subclass from {@linkplain AbstractQueueListener}.
 * @author esutdal
 * @see AbstractQueueListener
 * @param <T>
 */
public interface QueueListener<T extends Data> extends Consumer<T> {

	/**
	 * The type of {@linkplain Data} this listener is receiving.
	 * @return
	 */
	Class<T> dataType();
	/**
	 * Unique identifier for this queue listener
	 * @return
	 */
	String identifier();
	/**
	 * Max parallelism to achieve
	 * @return
	 */
	int concurrency();
	/**
	 * The exchange type
	 * @return
	 */
	String exchange();
	/**
	 * Max redelivery attempts.
	 * @return
	 */
	short maxDeliveryAttempts();
	
	/**
	 * Exception handler for listeners.
	 * @param e
	 */
	void onExceptionCaught(Throwable e);
	
	/**
	 * Check if a record is eligible for re-delivery based on expiration/delivery count. The {@linkplain Data} instance
	 * may be null.
	 * @param expired
	 * @param redeliveryCount
	 * @param d
	 * @return
	 */
	boolean allowRedelivery(boolean expired, short redeliveryCount, Data d);

}