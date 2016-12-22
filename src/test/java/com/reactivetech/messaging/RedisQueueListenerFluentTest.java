package com.reactivetech.messaging;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.blaze.queue.QueueService;
import com.blaze.queue.consume.Consumer;
import com.blaze.queue.consume.QueueListener;
import com.blaze.queue.consume.QueueListenerBuilder;
import com.blaze.queue.data.TextData;
import com.blaze.queue.redis.Blaze;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {Blaze.class})
public class RedisQueueListenerFluentTest {

	@Autowired
	QueueService service;
	static final Logger log = LoggerFactory.getLogger(RedisQueueListenerFluentTest.class);
	@Test
	public void pollFromQueue()
	{
		int n = service.size(SimpleQueueListener.QNAME);
		final CountDownLatch l = new CountDownLatch(n);
		
		QueueListener<TextData> abs = new QueueListenerBuilder()
		.concurrency(4)
		.consumer(new Consumer<TextData>() {

			@Override
			public void onMessage(TextData m) throws Exception {
				log.info("Recieved message ... " + m.getPayload());
				//log.info("CorrId ... " + m.getCorrelationID());
				l.countDown();
				
			}
		})
		.route(SimpleQueueListener.QNAME)
		.dataType(TextData.class)
		.build();
		
		long start = System.currentTimeMillis();
		service.registerListener(abs);
		
		try {
			boolean b = l.await(300, TimeUnit.SECONDS);
			Assert.assertTrue(b);
		} catch (InterruptedException e) {
			
		}
		
		long time = System.currentTimeMillis() - start;
		long secs = TimeUnit.MILLISECONDS.toSeconds(time);
		log.info("Time taken: " + secs + " secs " + (time - TimeUnit.SECONDS.toMillis(secs)) + " ms");
	}
}
