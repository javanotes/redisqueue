/**
 * Copyright 2016 esutdal

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.blaze.mq.redis.reqres;

import java.util.concurrent.TimeUnit;

import com.blaze.mq.Data;

public interface RequestReplyReceiver {

	/**
	 * Put the reply message for a request. Clients waiting on {@link #awaitAndGet()}, {@link #get()}
	 * will be notified accordingly.
	 * @param d
	 */
	void put(Data d);
	/**
	 * Await and get uninterruptibly.
	 * @param corrId
	 * @param maxWait
	 * @param unit
	 * @return The reply {@linkplain Data} if available, or null.
	 */
	Data awaitAndGet(String corrId, long maxWait, TimeUnit unit);

	/**
	 * Await and get. This will block indefinitely.
	 * @param corrId
	 * @return The reply {@linkplain Data} if available, or null.
	 * @throws InterruptedException
	 */
	Data get(String corrId) throws InterruptedException;

	/**
	 * Get and returns immediately.
	 * @param corrId
	 * @return The reply {@linkplain Data} if available, or null.
	 */
	Data getIfPresent(String corrId);

}