package com.reactivetechnologies.mq.server;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/*
 * -------------------------------------------------------
 *  REDIS Config.
 *  Refer to http://oldblog.antirez.com/post/redis-persistence-demystified.html for an understanding on the persistence in Redis.
 *  This can significantly impact the performance.
 *  
 *  Minimal changes advised, over the default Redis config:
 *  	appendonly yes
 *  	appendfsync everysec
 *  
 *  For a comprehensive understanding, please study Redis tuning in general.
 * ------------------------------------------------------- 
 */
@Configuration
public class BlazeConfig {
	@Bean
	BlazeRedisTemplate template(RedisConnectionFactory connectionFactory)
	{
		BlazeRedisTemplate rt = new BlazeRedisTemplate();
		rt.setConnectionFactory(connectionFactory);
		rt.setSerializers();
		return rt;
	}
	@Bean
	JarFilesDeployer deployer()
	{
		return new JarFilesDeployer();
	}
				
}
