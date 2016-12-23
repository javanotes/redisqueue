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
package com.blaze.mq.container;

import java.util.concurrent.TimeoutException;

import com.blaze.mq.redis.core.QRecord;

public interface QueueContainerTask {

	/**
	 * Fire callback on consumer.
	 * @param qr
	 */
	void fireOnMessage(QRecord qr);

	/**
	 * Perform a blocking fetch for queue head.
	 * @return
	 * @throws TimeoutException
	 */
	QRecord fetchHead() throws TimeoutException;

}