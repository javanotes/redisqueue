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
package com.reactivetechnologies.blaze.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.reactivetechnologies.blaze.handlers.ConsumerRecoveryHandler;
import com.reactivetechnologies.blaze.ops.DataAccessor;
//TODO
public class DefaultConsumerRecoveryHandler implements ConsumerRecoveryHandler {
	/**
	 * 
	 * @param redisOps
	 */
	public DefaultConsumerRecoveryHandler() {
		super();
	}
	@Value("${blaze.instance.id}")
	private String instanceId;
	@Autowired
	private DataAccessor redisOps;
	private static final Logger log = LoggerFactory.getLogger(DefaultConsumerRecoveryHandler.class);
	@Override
	public void handle(String exchange, String route) {
		String listKey = redisOps.prepareInProcKey(exchange, route);
		log.info("Checking in-processing messages for exchange '"+exchange+"', route '"+route+"'");
	}

}
