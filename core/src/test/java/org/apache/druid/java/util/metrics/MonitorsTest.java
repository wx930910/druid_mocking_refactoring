/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.java.util.metrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

public class MonitorsTest {

	@Test
	public void testSetFeed() {
		String feed = "testFeed";
		StubServiceEmitter emitter = new StubServiceEmitter("dev/monitor-test", "localhost:0000");
		Monitor m = Monitors.createCompoundJvmMonitor(ImmutableMap.of(), feed);
		m.start();
		m.monitor(emitter);
		m.stop();
		checkEvents(emitter.getEvents(), feed);
	}

	@Test
	public void testDefaultFeed() {
		List<Event> events = new ArrayList<>();
		ServiceEmitter emitter = Mockito.spy(new ServiceEmitter("dev/monitor-test", "localhost:0000", null));
		Mockito.doNothing().when(emitter).start();
		try {
			Mockito.doNothing().when(emitter).flush();
			Mockito.doNothing().when(emitter).close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		Mockito.doAnswer(invo -> {
			Event event = invo.getArgument(0);
			events.add(event);
			return null;
		}).when(emitter).emit(Mockito.any(Event.class));
		// StubServiceEmitter emitter = new
		// StubServiceEmitter("dev/monitor-test", "localhost:0000");
		Monitor m = Monitors.createCompoundJvmMonitor(ImmutableMap.of());
		m.start();
		m.monitor(emitter);
		m.stop();
		checkEvents(events, "metrics");
	}

	private void checkEvents(List<Event> events, String expectedFeed) {
		Assert.assertFalse("no events emitted", events.isEmpty());
		for (Event e : events) {
			if (!expectedFeed.equals(e.getFeed())) {
				String message = StringUtils.format("\"feed\" in event: %s", e.toMap().toString());
				Assert.assertEquals(message, expectedFeed, e.getFeed());
			}
		}
	}
}
