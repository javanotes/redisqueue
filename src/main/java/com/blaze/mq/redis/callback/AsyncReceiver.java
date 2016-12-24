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
package com.blaze.mq.redis.callback;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.blaze.mq.Data;
import com.blaze.mq.consume.Consumer;
import com.blaze.mq.consume.QueueListener;
import com.blaze.mq.consume.QueueListenerBuilder;
import com.blaze.mq.data.TextData;

//TODO: WIP
@Component
public class AsyncReceiver {

	public static final String ASYNC_REPLYQ = "COMMON_REPLYQ";
	private QueueListener<Data> listener;
	private static final Logger log = LoggerFactory.getLogger(AsyncReceiver.class);
	public AsyncReceiver() {
		
	}
	private Map<String, Object> waitMap = new HashMap<>();
	@PostConstruct
	private void init()
	{
		listener = new QueueListenerBuilder()
		.concurrency(Runtime.getRuntime().availableProcessors()*2)
		.consumer(new Consumer<Data>() {

			@Override
			public void onMessage(Data m) throws Exception {
				if(waitMap.containsKey(m.getCorrelationID()))
				{
					Object o = waitMap.get(m.getCorrelationID());
					synchronized (o) {
						o.notify();
					}
					waitMap.remove(m.getCorrelationID());
				}
				
			}
		})
		.route(ASYNC_REPLYQ)
		.dataType(Data.class)
		.build();
	}

}
