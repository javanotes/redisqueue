package com.reactivetechnologies.mq.server;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication(scanBasePackageClasses = {BlazeConfig.class})
public class Blaze {

	public static void main(String[] args) {
		new SpringApplicationBuilder()
	    .sources(Blaze.class)
	    //.bannerMode(org.springframework.boot.Banner.Mode.OFF)
	    .run(args);
	}
}
