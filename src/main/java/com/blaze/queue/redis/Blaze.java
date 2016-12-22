package com.blaze.queue.redis;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.cassandra.CassandraDataAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication(scanBasePackageClasses = {Config.class}, exclude = {CassandraDataAutoConfiguration.class})
public class Blaze {

	public static void main(String[] args) {
		new SpringApplicationBuilder()
	    .sources(Blaze.class)
	    //.bannerMode(org.springframework.boot.Banner.Mode.OFF)
	    .run(args);
	}
}
