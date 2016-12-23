package com.reactivetech.messaging;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.blaze.mq.QueueService;
import com.blaze.mq.consume.Consumer;
import com.blaze.mq.consume.QueueListener;
import com.blaze.mq.consume.QueueListenerBuilder;
import com.blaze.mq.data.TextData;
import com.blaze.mq.redis.Blaze;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {Blaze.class})
public class RedisQueueListenerFluentTest2 {

	@Autowired
	QueueService service;
	static final Logger log = LoggerFactory.getLogger(RedisQueueListenerFluentTest2.class);
	static String QNAME = "QWITHREDELIVERY";
	static String PAYLOAD = "Message9.0x";
	@Before
	public void publish()
	{
		service.clear(QNAME);
		service.add(Arrays.asList(new TextData(PAYLOAD, QNAME)));
	}
	@After
	public void checkDeadLettered()
	{
		int llen = service.size(QNAME);
		Assert.assertEquals(0, llen);
	}
	@Test
	public void testWithRedelivery()
	{
		
		CountDownLatch l = new CountDownLatch(3);
		QueueListener<TextData> abs = new QueueListenerBuilder()
		.concurrency(4)
		.consumer(new Consumer<TextData>() {

			@Override
			public void onMessage(TextData m) throws Exception {
				log.info("Recieved message ... " + m.getPayload());
				l.countDown();
				throw new IllegalArgumentException("Dummy exception raised");
				
			}
		})
		.route(QNAME)
		.dataType(TextData.class)
		.build();
		
		long start = System.currentTimeMillis();
		service.registerListener(abs);
		
		try {
			boolean b = l.await(10, TimeUnit.SECONDS);
			Assert.assertTrue(b);
		} catch (InterruptedException e) {
			
		}
		
		long time = System.currentTimeMillis() - start;
		long secs = TimeUnit.MILLISECONDS.toSeconds(time);
		log.info("Time taken: " + secs + " secs " + (time - TimeUnit.SECONDS.toMillis(secs)) + " ms");
	}
}
