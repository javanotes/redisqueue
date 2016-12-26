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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.blaze.mq.Data;
import com.blaze.mq.consume.Consumer;
import com.blaze.mq.consume.QueueListener;
import com.blaze.mq.consume.QueueListenerBuilder;
import com.blaze.mq.container.QueueContainer;
/**
 * Asynchronous reply queue listener. This component should be used to query for a response {@linkplain Data} in a request-reply
 * semantic. Internally uses a {@linkplain ConcurrentHashMap} to cache the response atomically.
 * @author esutdal
 *
 */
//TODO: WIP
@Component
//@ConditionalOnProperty(name = "blaze.request-reply.enable", havingValue = "true")
public class AsyncReplyReceiver {

	@Autowired
	private QueueContainer container;
	@Autowired
	private DiskCacheLoader diskCache;
	/**
	 * The reply listener class.
	 * @author esutdal
	 *
	 */
	private final class ReplyListener implements Consumer<Data> {
		/**
		 * Put data to the reply cache atomically.
		 * @param m
		 * @param corrId
		 * @param o
		 */
		private void putToReplyCache(Data m, String corrId, Object o)
		{
			try 
			{

				if ((resultCache.putIfAbsent(corrId, m)) != null) 
				{
					//there is a thread waiting for notification which has set the dummy data
					boolean replaced = resultCache.replace(corrId, dummyData, m);
					if (!replaced) {
						synchronized (o) {
							//the put will always be true, since the waiting thread will enter the monitor only if it 
							//has successfully removed the dummy data
							Data d = resultCache.putIfAbsent(corrId, m);
							Assert.isNull(d, "Dummy was not removed when result is placed");
							o.notify();
						}
					}
				}

			} finally {
				waitMap.remove(corrId, o);
			}
		}
		@Override
		public void onMessage(Data m) throws Exception 
		{
			if(destroyed.compareAndSet(false, false)) 
			{
				String corrId = m.getCorrelationID();
				waitMap.putIfAbsent(corrId, new Object());
				Object o = waitMap.get(corrId);
				
				putToReplyCache(m, corrId, o);
			}
			else
			{
				log.warn("Listener is shutting down. Dumping message with corrId => "+m.getCorrelationID());
				diskCache.dumpToDisk(m);
			}
			
		}

		@Override
		public void destroy() {
			destroyed.compareAndSet(false, true);
			dumpResultCache();
		}
		@Override
		public void init() {
			diskCache.loadFromDisk();
		}
	}
	/**
	 * The common reply queue, for all request-response invocations.
	 * 
	 */
	public static final String ASYNC_REPLYQ = "COMMON_REPLYQ";
	private QueueListener<Data> listener;
	private static final Logger log = LoggerFactory.getLogger(AsyncReplyReceiver.class);
	public AsyncReplyReceiver() {
		
	}
	
	private ConcurrentMap<String, Object> waitMap = new ConcurrentHashMap<>();
	private ConcurrentMap<String, Data> resultCache = new ConcurrentHashMap<>();
	
	private final Data dummyData = new Data() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
	};
	/**
	 * Await and get uninterruptibly.
	 * @param corrId
	 * @param maxWait
	 * @param unit
	 * @return The reply {@linkplain Data} if available, or null.
	 */
	public Data awaitAndGet(String corrId, long maxWait, TimeUnit unit)
	{
		Assert.isTrue(destroyed.compareAndSet(false, false), "Rejected on shutting down");
		Assert.notNull(corrId);
		Assert.isTrue(maxWait > 0, "Expectings maxWait > 0");
		try {
			return get0(corrId, maxWait, unit);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return null;
	}
	/**
	 * Await and get. This will block indefinitely.
	 * @param corrId
	 * @return The reply {@linkplain Data} if available, or null.
	 * @throws InterruptedException
	 */
	public Data awaitAndGet(String corrId) throws InterruptedException
	{
		Assert.isTrue(destroyed.compareAndSet(false, false), "Rejected on shutting down");
		Assert.notNull(corrId);
		return get0(corrId, 0, TimeUnit.MILLISECONDS);
	}
	/**
	 * Get and returns immediately.
	 * @param corrId
	 * @return The reply {@linkplain Data} if available, or null.
	 */
	public Data get(String corrId)
	{
		Assert.isTrue(destroyed.compareAndSet(false, false), "Rejected on shutting down");
		Assert.notNull(corrId);
		try {
			return get0(corrId, -1, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return null;
	}
	/**
	 * If maxWait = 0, wait indefinitely; If maxWait > 0, wait for specified time period; 
	 * @param corrId
	 * @param maxWait
	 * @param unit
	 * @throws InterruptedException
	 */
	private void awaitOnGet(String corrId, long maxWait, TimeUnit unit) throws InterruptedException
	{
		//result has not been put, and the dummy data has been removed now
		waitMap.putIfAbsent(corrId, new Object());
		Object o = waitMap.get(corrId);
		synchronized (o) {
			//wait for some time, and if possible get notified
			if (!resultCache.containsKey(corrId)) {
				if (maxWait > 0) {
					o.wait(unit.toMillis(maxWait));
				}
				else
				{
					o.wait();
				}
			}
		}
	}
	/**
	 * Await and get atomically. If maxWait = 0, wait indefinitely; If maxWait > 0, wait for specified time period; 
	 * If maxWait < 0, return immediately.
	 * @param corrId
	 * @param maxWait
	 * @param unit
	 * @return
	 * @throws InterruptedException
	 */
	private Data getAtomic(String corrId, long maxWait, TimeUnit unit) throws InterruptedException
	{
		//if remove is successful that means result has not been put yet
		boolean dummyWasRemoved = resultCache.remove(corrId, dummyData);
		if(dummyWasRemoved)
		{
			if (maxWait >= 0) {
				awaitOnGet(corrId, maxWait, unit); 
			}
		}
		
		return resultCache.get(corrId);
	}
	/**
	 * Await and get result from result cache atomically. Uses {@linkplain ConcurrentMap} atomic
	 * operations. 
	 * @param corrId correlation Id
	 * @param maxWait maximum wait
	 * @param unit unit
	 * @return The reply {@linkplain Data} if available, or null.
	 * @throws InterruptedException
	 */
	private Data get0(String corrId, long maxWait, TimeUnit unit) throws InterruptedException
	{
		Data d = resultCache.putIfAbsent(corrId, dummyData);
		
		//putIfAbsent succeeded. so the result was not yet put in cache.
		if(d == null)
		{
			d = getAtomic(corrId, maxWait, unit);
		}
		
		if(d == null)
			return null;
		
		//result is present in cache. now remove it from cache and return.
		boolean removed = resultCache.remove(corrId, d);
		if(!removed){
			log.debug("Removed was false for corrId => "+corrId+". Returning null;");
		}
		
		return d;
	}
	private AtomicBoolean destroyed = new AtomicBoolean();
	
	private void dumpResultCache()
	{
		while(!destroyed.compareAndSet(true, true));
		if(!resultCache.isEmpty())
		{
			for(Data d : resultCache.values())
			{
				if(d != dummyData)
					diskCache.dumpToDisk(d);
			}
		}
	}
	@PostConstruct
	private void init()
	{
		listener = new QueueListenerBuilder()
		.concurrency(Runtime.getRuntime().availableProcessors())
		.consumer(new ReplyListener())
		.sharedPool(false)
		.identifier("AsyncListener")
		.route(ASYNC_REPLYQ)
		.dataType(Data.class)
		.build();
		
		container.register(listener);
		
		log.info("Async reply listener started ..");
	}

}
