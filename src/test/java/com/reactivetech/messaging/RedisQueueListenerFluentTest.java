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

import com.reactivetechnologies.mq.QueueService;
import com.reactivetechnologies.mq.consume.Consumer;
import com.reactivetechnologies.mq.consume.QueueListener;
import com.reactivetechnologies.mq.consume.QueueListenerBuilder;
import com.reactivetechnologies.mq.container.QueueContainer;
import com.reactivetechnologies.mq.data.TextData;
import com.reactivetechnologies.mq.server.Blaze;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {Blaze.class})
public class RedisQueueListenerFluentTest {

	@Autowired
	QueueService service;
	@Autowired
	QueueContainer container;
	static final Logger log = LoggerFactory.getLogger(RedisQueueListenerFluentTest.class);
	@Test
	public void pollFromQueue()
	{
		int n = service.size(SimpleQueueListener.QNAME);
		final CountDownLatch l = new CountDownLatch(n);
		log.info("MESSAGE TO FETCH => "+n);
		QueueListener<TextData> abs = new QueueListenerBuilder()
		.concurrency(8)
		.consumer(new Consumer<TextData>() {

			@Override
			public void onMessage(TextData m) throws Exception {
				log.info("Recieved message ... " + m.getPayload());
				//log.info("CorrId ... " + m.getCorrelationID());
				l.countDown();
				
			}

			@Override
			public void destroy() {
				log.info("RedisQueueListenerFluentTest.pollFromQueue().new Consumer() {...}.destroy()");
				
			}

			@Override
			public void init() {
				log.info("RedisQueueListenerFluentTest.pollFromQueue().new Consumer() {...}.init()");
			}
		})
		.route(SimpleQueueListener.QNAME)
		.dataType(TextData.class)
		.build();
		
		long start = System.currentTimeMillis();
		container.register(abs);
		
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
