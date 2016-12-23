package com.blaze.mq.redis.core;

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
import org.springframework.stereotype.Component;

import com.blaze.mq.Data;
import com.blaze.mq.consume.AbstractQueueListener;
import com.blaze.mq.container.QueueContainer;
import com.blaze.mq.redis.ops.DataAccessor;
/**
 * The core container that manages listener task execution. This class
 * is responsible for scheduling the worker threads amongst the listeners and
 * maintain thread-safety and other concurrency guarantee.
 * @author esutdal
 *
 */
@Component
public class BlazeQueueContainer implements Runnable, QueueContainer{

	private static final Logger log = LoggerFactory.getLogger(BlazeQueueContainer.class);
	
	@Autowired
	private DataAccessor redisOps;
	
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
	public void destroy()
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
		String preparedKey = DataAccessor.prepareHashKey(qr.getKey().getExchange(), qr.getKey().getRoutingKey());
		redisOps.endCommit(qr, preparedKey);
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
		log.warn("+----------------------+");
		log.warn("|TODO:DEAD_LETTER_NOTIF|");
		log.warn("+----------------------+");
	}
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.IQueueListenerContainer#rollback(com.reactivetech.messaging.cmq.dao.QRecord)
	 */
	@Override
	public void rollback(QRecord qr) {
		String preparedKey = DataAccessor.prepareHashKey(qr.getKey().getExchange(), qr.getKey().getRoutingKey());
		redisOps.endCommit(qr, preparedKey);
		preparedKey = DataAccessor.prepareListKey(qr.getKey().getExchange(), qr.getKey().getRoutingKey());
		redisOps.enqueueHead(qr, preparedKey);
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

	QRecord fetchHead(String exchange, String routing, long pollInterval2, TimeUnit milliseconds) throws TimeoutException {
		return redisOps.fetchHead(exchange, routing, pollInterval2, milliseconds);
	}
}
