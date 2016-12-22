package com.blaze.queue.redis.core;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.stereotype.Component;

import com.blaze.queue.Data;
import com.blaze.queue.consume.AbstractQueueListener;
import com.blaze.queue.container.QueueContainer;
import com.blaze.queue.domain.QRecord;
import com.blaze.queue.redis.BlazeRedisTemplate;
/**
 * The core container that manages listener task execution. This class
 * is responsible for scheduling the worker threads amongst the listeners and
 * maintain thread-safety and other concurrency guarantee.
 * @author esutdal
 *
 */
@Component
class BlazeQueueContainer implements Runnable, QueueContainer{

	private static final Logger log = LoggerFactory.getLogger(BlazeQueueContainer.class);
	/*
	 * NOTE: Redis keys are data structure specific. So you cannot use the same key for hash and list.
	 */
	/**
	 * Prepare a key based on the exchange and route information. Presently it is
	 * simply appended.
	 * @param exchange
	 * @param key
	 * @return
	 */
	static String prepareKey(String exchange, String key)
	{
		return new StringBuilder().append(exchange).append("-").append(key).toString();
	}
	/**
	 * 
	 * @param exchange
	 * @param key
	 * @return
	 */
	static String prepareHashKey(String exchange, String key)
	{
		return new StringBuilder().append(exchange).append("#").append(key).toString();
	}
	@Autowired
	BlazeRedisTemplate redisTemplate;
	
	private ForkJoinPool threadPool;
	@Value("${consumer.worker.thread:0}")
	private int fjWorkers;
	
	@PostConstruct
	void init()
	{
		threadPool = new ForkJoinPool(fjWorkers <= 0 ? Runtime.getRuntime().availableProcessors() : fjWorkers, new ForkJoinWorkerThreadFactory() {
		      
		      private int containerThreadCount = 0;

			@Override
		      public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
		        ForkJoinWorkerThread t = new ForkJoinWorkerThread(pool){
		          
		        };
		        t.setName("Container-Worker-"+(containerThreadCount ++));
		        return t;
		      }
		    }, new UncaughtExceptionHandler() {
		      
		      @Override
		      public void uncaughtException(Thread t, Throwable e) {
		        log.error("-- Uncaught exception in fork join pool --", e);
		        
		      }
		    }, true);
		running = true;
		log.info("Container initialized with fj workers "+threadPool.getParallelism() + " fj poolSize "+threadPool.getPoolSize());
	}
	
	@PreDestroy
	void destroy()
	{
		running = false;
		threadPool.shutdown();
		try {
			threadPool.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			
		}
		log.info("Container stopped..");
	}
	private final Set<AbstractQueueListener<? extends Data>> listeners = new ConcurrentSkipListSet<>();
	
	public QRecord fetchHead(String xchng, String route, long await, TimeUnit unit) throws TimeoutException
	{
		String preparedKey = prepareKey(xchng, route);
		BoundListOperations<String, QRecord> listOps = redisTemplate.boundListOps(preparedKey);
		QRecord qr = listOps.leftPop(await, unit);
		if(qr != null)
		{
			//not making this asynchronous to maintain state integrity
			try {
				prepareCommit(qr, prepareHashKey(xchng, route));
			} catch (Exception e) {
				log.warn("** Message id "+qr.getKey().getTimeuid()+" not prepared for commit. So redelivery will not work! **");
				log.warn("Root cause: "+e.getMessage());
				log.debug("", e);
			}
			
			return qr;
		}
		throw new TimeoutException();
		
	}
	/*
	 * Note: On inspecting the Spring template bound*Ops() in Spring code, 
	 * they seem to be plain wrapper classes exposing a subset of operations (restrictive decorator?), 
	 * and passing on the execution to the proxied template class which is a singleton. 
	 * Thus creating large number of such local instances should not be very expensive.
	 * 
	 * This is purely from a theoretical point of view. If profiling suggests otherwise, then caching the
	 * instances can always be considered.
	 */
	
	/**
	 * 
	 * @param qr
	 * @param key
	 */
	private void prepareCommit(QRecord qr, String key) {
		BoundHashOperations<String, String, QRecord> hashOps = redisTemplate.boundHashOps(key);
		hashOps.put(qr.getKey().getTimeuid().toString(), qr);
	}
	/**
	 * 
	 * @param qr
	 * @param key
	 */
	private void endCommit(QRecord qr, String key) {
		BoundHashOperations<String, String, QRecord> hashOps = redisTemplate.boundHashOps(key);
		hashOps.delete(qr.getKey().getTimeuid().toString());
	}
	private volatile boolean running;
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.IQueueListenerContainer#register(com.reactivetech.messaging.cmq.core.AbstractQueueListener)
	 */
	@Override
	public <T extends Data> void register(AbstractQueueListener<T> listener)
	{
		if(!running){
			listeners.add(listener);
		}
		else
			run(listener);
		
		log.info("Added listener "+listener);
	}
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.IQueueListenerContainer#run()
	 */
	@Override
	public void run() {
		log.debug("running poll task for tasks " + listeners.size());
		for (AbstractQueueListener<? extends Data> task : listeners) {
			run(task);
		}
		
	}
	/**
	 * Run the specified listener.
	 * @param task
	 */
	private void run(AbstractQueueListener<? extends Data> task) {
		if(running)
		{
			threadPool.execute(new BlazeQueueContainerTask<>(task, this));
		}
		
	}
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.IQueueListenerContainer#commit(com.reactivetech.messaging.cmq.dao.QRecord, boolean)
	 */
	@Override
	public void commit(QRecord qr, boolean success) {
		String preparedKey = prepareHashKey(qr.getKey().getExchange(), qr.getKey().getRoutingKey());
		endCommit(qr, preparedKey);
		if(!success)
		{
			//message being lost
			recordDeadLetter(qr);
		}
	}
	/**
	 * Handle messages discarded after retry limit exceeded or expiration or unknown cause.
	 * @param qr
	 */
	protected void recordDeadLetter(QRecord qr) {
		log.warn("TODO: DEAD_LETTER_NOTIF "+qr);
	}
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.IQueueListenerContainer#rollback(com.reactivetech.messaging.cmq.dao.QRecord)
	 */
	@Override
	public void rollback(QRecord qr) {
		String preparedKey = prepareHashKey(qr.getKey().getExchange(), qr.getKey().getRoutingKey());
		endCommit(qr, preparedKey);
		enqueueHead(qr, preparedKey);
	}
	private void enqueueHead(QRecord qr, String preparedKey) {
		BoundListOperations<String, QRecord> listOps = redisTemplate.boundListOps(preparedKey);
		listOps.leftPush(qr);
	}
	private long pollInterval;
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.IQueueListenerContainer#getPollInterval()
	 */
	@Override
	public long getPollInterval() {
		return pollInterval;
	}
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.IQueueListenerContainer#setPollInterval(long)
	 */
	@Override
	public void setPollInterval(long pollInterval) {
		this.pollInterval = pollInterval;
	}
}
