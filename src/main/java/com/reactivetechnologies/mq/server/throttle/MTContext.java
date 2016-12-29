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
package com.reactivetechnologies.mq.server.throttle;

import org.apache.commons.chain.impl.ContextBase;

import com.reactivetechnologies.mq.server.handlers.ConsumerThrottlingHandler;

class MTContext extends ContextBase {

	public MTContext(int throttlingTPS, ConsumerThrottlingHandler throttler) {
		super();
		this.throttlingTPS = throttlingTPS;
		this.throttler = throttler;
	}
	private final int throttlingTPS;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private boolean throttle;
	
	private final ConsumerThrottlingHandler throttler;
	public int getThrottlingTPS() {
		return throttlingTPS;
	}


	public boolean isThrottle() {
		return throttle;
	}


	public void setThrottle(boolean throttle) {
		this.throttle = throttle;
	}


	public ConsumerThrottlingHandler getThrottler() {
		return throttler;
	}


	

}
