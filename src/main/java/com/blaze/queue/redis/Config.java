package com.blaze.queue.redis;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/*
 * -------------------------------------------------------
 *  REDIS Config
 * ------------------------------------------------------- 
 */
@Configuration
public class Config {
	@Bean
	BlazeRedisTemplate template(RedisConnectionFactory connectionFactory)
	{
		BlazeRedisTemplate rt = new BlazeRedisTemplate();
		rt.setConnectionFactory(connectionFactory);
		rt.setSerializers();
		return rt;
	}
			
}
