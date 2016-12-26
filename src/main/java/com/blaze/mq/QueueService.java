/* ============================================================================
*
* FILE: IQueueService.java
*
The MIT License (MIT)

Copyright (c) 2016 Sutanu Dalui

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*
* ============================================================================
*/
package com.blaze.mq;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.blaze.mq.redis.core.QRecord;

public interface QueueService {

	String DEFAULT_XCHANGE = "default";
	
	/**
	 * Adds new message to the default exchange and given routing key
	 * 
	 * @param msg
	 * @return the count of successful publish
	 */
	<T extends Data> int add(List<T> msg);

	/**
	 * Adds new message to the given exchange and given routing key
	 * 
	 * @param msg
	 * @param xchangeKey
	 * @return the count of successful publish
	 */
	<T extends Data> int add(List<T> msg, String xchangeKey);
	
	/**
	 * Adds new message to the default exchange and given routing key asynchronously.
	 * 
	 * @param msg
	 */
	<T extends Data> void ingest(List<T> msg);

	/**
	 * Adds new message to the given exchange and given routing key asynchronously.
	 * 
	 * @param msg
	 * @param xchangeKey
	 */
	<T extends Data> void ingest(List<T> msg, String xchangeKey);

	/**
	 * Queue backlog for given routing key in default exchange
	 * 
	 * @param q
	 * @return
	 */
	Integer size(String routeKey);

	/**
	 * Queue backlog for given exchange/routing key
	 * 
	 * @param q
	 * @return
	 */
	Integer size(String xchangeKey, String routeKey);
	
	/**
	 * Clears backlog for given routing key in default exchange
	 * 
	 * @param q
	 */
	void clear(String routeKey);

	/**
	 * Clears backlog for given routing key in given exchange
	 * 
	 * @param xchangeKey
	 * @param routeKey
	 */
	void clear(String xchangeKey, String routeKey);

	/**
	 * Fetch head of the queue.
	 * @param xchng
	 * @param route
	 * @param await
	 * @param unit
	 * @return
	 * @throws TimeoutException
	 */
	QRecord getNext(String xchng, String route, long await, TimeUnit unit) throws TimeoutException;

}