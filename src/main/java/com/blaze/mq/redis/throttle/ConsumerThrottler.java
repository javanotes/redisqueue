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
package com.blaze.mq.redis.throttle;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.chain.Command;
import org.apache.commons.chain.impl.ChainBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.blaze.mq.common.BlazeInternalError;
@Component
public class ConsumerThrottler extends ChainBase{

	@Value("${consumer.throttle.tps.millis:1000}")
	private long throttlerPeriod;
	@Value("${consumer.throttle.enable:true}")
	private boolean enabled;
	private ScheduledExecutorService timer;
	private AtomicInteger counter;
	private static final Logger log = LoggerFactory.getLogger(ConsumerThrottler.class);
	
	private class MessageThrottlerTask implements Runnable
	{
		
		@Override
		public void run() {
			while(!counter.compareAndSet(getCount(), 0));
			log.debug("Set");
		}
		
	}
	/**
	 * 
	 */
	public ConsumerThrottler() {
		super();
	}
	/**
	 * Provision to add more commands. The first command, which is mandatory and system defined, will set the current count in context.
	 * So subsequent commands can refer to it if needed.
	 */
	protected List<Command> loadCommands()
	{
		//NOOP
		return Collections.emptyList();
	}
	@PostConstruct
	private void init()
	{
		if (enabled) 
		{
			addCommand(new ThrottleCommand());
			List<Command> customCommands = loadCommands();
			if(customCommands != null)
			{
				for(Command cmd : customCommands)
					addCommand(cmd);
			}
			
			counter = new AtomicInteger();
			timer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

				@Override
				public Thread newThread(Runnable arg0) {
					return new Thread(arg0, "Throttler-timer-thread");
				}
			});
			timer.scheduleWithFixedDelay(new MessageThrottlerTask(), 0, throttlerPeriod, TimeUnit.MILLISECONDS);
		}
	}
	@PreDestroy
	private void destroy()
	{
		if (timer != null) {
			timer.shutdown();
			try {
				timer.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				
			}
		}
	}
	int getCount()
	{
		return counter.addAndGet(0);
	}
	/**
	 * 
	 */
	public void incrementCount()
	{
		if (enabled) {
			counter.incrementAndGet();
		}
	}
	/**
	 * Check if message consumption needs to be throttled.
	 * @param throttleTps
	 * @return true, if consumption will be allowed
	 * @throws Exception
	 */
	public boolean allowMessageConsume(int throttleTps) 
	{
		if(!enabled)
			return true;
		MTContext ctx = new MTContext(throttleTps, this);
		try {
			execute(ctx);
		} catch (Exception e) {
			throw new BlazeInternalError("Exception in message throtller", e);
		}
		return !ctx.isThrottle();
		
	}

}
