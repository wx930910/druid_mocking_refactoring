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

import java.util.List;

import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

public class JvmMonitorTest {

	@Test(timeout = 60_000L)
	public void testGcCounts() throws InterruptedException {
		// GcTrackingEmitter emitter = new GcTrackingEmitter();
		Number[] oldGcCount = new Number[1];
		Number[] oldGcCpu = new Number[1];
		Number[] youngGcCount = new Number[1];
		Number[] youngGcCpu = new Number[1];
		Emitter emitter = Mockito.mock(Emitter.class);
		Mockito.doAnswer(invo -> {
			Event e = invo.getArgument(0);
			ServiceMetricEvent event = (ServiceMetricEvent) e;
			String gcGen = null;
			if (event.toMap().get("gcGen") != null) {
				gcGen = ((List) event.toMap().get("gcGen")).get(0).toString();
			}

			switch (event.getMetric() + "/" + gcGen) {
			case "jvm/gc/count/old":
				oldGcCount[0] = event.getValue();
				break;
			case "jvm/gc/cpu/old":
				oldGcCpu[0] = event.getValue();
				break;
			case "jvm/gc/count/young":
				youngGcCount[0] = event.getValue();
				break;
			case "jvm/gc/cpu/young":
				youngGcCpu[0] = event.getValue();
				break;
			}
			return null;
		}).when(emitter).emit(Mockito.any());
		final ServiceEmitter serviceEmitter = new ServiceEmitter("test", "localhost", emitter);
		serviceEmitter.start();
		final JvmMonitor jvmMonitor = new JvmMonitor();
		// skip tests if gc counters fail to initialize with this JDK
		Assume.assumeNotNull(jvmMonitor.gcCounters);

		while (true) {
			// generate some garbage to see gc counters incremented
			@SuppressWarnings("unused")
			byte[] b = new byte[1024 * 1024 * 50];
			reset(oldGcCount, oldGcCpu, youngGcCount, youngGcCpu);
			jvmMonitor.doMonitor(serviceEmitter);
			if (gcSeen(oldGcCount, oldGcCpu, youngGcCount, youngGcCpu)) {
				return;
			}
			Thread.sleep(10);
		}
	}

	private void reset(Number[] oldGcCount, Number[] oldGcCpu, Number[] youngGcCount, Number[] youngGcCpu) {
		oldGcCount = new Number[1];
		oldGcCpu = new Number[1];
		youngGcCount = new Number[1];
		youngGcCpu = new Number[1];
	}

	private boolean oldGcSeen(Number[] oldGcCount, Number[] oldGcCpu, Number[] youngGcCount, Number[] youngGcCpu) {
		boolean oldGcCountSeen = oldGcCount != null && oldGcCount[0].longValue() > 0;
		boolean oldGcCpuSeen = oldGcCpu != null && oldGcCpu[0].longValue() > 0;
		if (oldGcCountSeen || oldGcCpuSeen) {
			System.out.println("old count: " + oldGcCount + ", cpu: " + oldGcCpu);
		}
		Assert.assertFalse("expected to see old gc count and cpu both zero or non-existent or both positive",
				oldGcCountSeen ^ oldGcCpuSeen);
		return oldGcCountSeen;
	}

	private boolean youngGcSeen(Number[] oldGcCount, Number[] oldGcCpu, Number[] youngGcCount, Number[] youngGcCpu) {
		boolean youngGcCountSeen = youngGcCount != null && youngGcCount[0].longValue() > 0;
		boolean youngGcCpuSeen = youngGcCpu != null && youngGcCpu[0].longValue() > 0;
		if (youngGcCountSeen || youngGcCpuSeen) {
			System.out.println("young count: " + youngGcCount + ", cpu: " + youngGcCpu);
		}
		Assert.assertFalse("expected to see young gc count and cpu both zero/non-existent or both positive",
				youngGcCountSeen ^ youngGcCpuSeen);
		return youngGcCountSeen;
	}

	private boolean gcSeen(Number[] oldGcCount, Number[] oldGcCpu, Number[] youngGcCount, Number[] youngGcCpu) {
		return oldGcSeen(oldGcCount, oldGcCpu, youngGcCount, youngGcCpu)
				|| youngGcSeen(oldGcCount, oldGcCpu, youngGcCount, youngGcCpu);
	}

	private static class GcTrackingEmitter implements Emitter {
		private Number oldGcCount;
		private Number oldGcCpu;
		private Number youngGcCount;
		private Number youngGcCpu;

		@Override
		public void start() {

		}

		void reset() {
			oldGcCount = null;
			oldGcCpu = null;
			youngGcCount = null;
			youngGcCpu = null;
		}

		@Override
		public void emit(Event e) {
			ServiceMetricEvent event = (ServiceMetricEvent) e;
			String gcGen = null;
			if (event.toMap().get("gcGen") != null) {
				gcGen = ((List) event.toMap().get("gcGen")).get(0).toString();
			}

			switch (event.getMetric() + "/" + gcGen) {
			case "jvm/gc/count/old":
				oldGcCount = event.getValue();
				break;
			case "jvm/gc/cpu/old":
				oldGcCpu = event.getValue();
				break;
			case "jvm/gc/count/young":
				youngGcCount = event.getValue();
				break;
			case "jvm/gc/cpu/young":
				youngGcCpu = event.getValue();
				break;
			}
		}

		boolean gcSeen() {
			return oldGcSeen() || youngGcSeen();
		}

		private boolean oldGcSeen() {
			boolean oldGcCountSeen = oldGcCount != null && oldGcCount.longValue() > 0;
			boolean oldGcCpuSeen = oldGcCpu != null && oldGcCpu.longValue() > 0;
			if (oldGcCountSeen || oldGcCpuSeen) {
				System.out.println("old count: " + oldGcCount + ", cpu: " + oldGcCpu);
			}
			Assert.assertFalse("expected to see old gc count and cpu both zero or non-existent or both positive",
					oldGcCountSeen ^ oldGcCpuSeen);
			return oldGcCountSeen;
		}

		private boolean youngGcSeen() {
			boolean youngGcCountSeen = youngGcCount != null && youngGcCount.longValue() > 0;
			boolean youngGcCpuSeen = youngGcCpu != null && youngGcCpu.longValue() > 0;
			if (youngGcCountSeen || youngGcCpuSeen) {
				System.out.println("young count: " + youngGcCount + ", cpu: " + youngGcCpu);
			}
			Assert.assertFalse("expected to see young gc count and cpu both zero/non-existent or both positive",
					youngGcCountSeen ^ youngGcCpuSeen);
			return youngGcCountSeen;
		}

		@Override
		public void flush() {

		}

		@Override
		public void close() {

		}
	}
}
