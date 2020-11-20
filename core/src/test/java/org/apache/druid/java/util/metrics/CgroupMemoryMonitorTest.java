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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.ProcCgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

public class CgroupMemoryMonitorTest {
	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
	private File procDir;
	private File cgroupDir;
	private CgroupDiscoverer discoverer;

	@Before
	public void setUp() throws IOException {
		cgroupDir = temporaryFolder.newFolder();
		procDir = temporaryFolder.newFolder();
		discoverer = new ProcCgroupDiscoverer(procDir.toPath());
		TestUtils.setUpCgroups(procDir, cgroupDir);
		final File memoryDir = new File(cgroupDir, "memory/system.slice/some.service");
		Assert.assertTrue((memoryDir.isDirectory() && memoryDir.exists()) || memoryDir.mkdirs());
		TestUtils.copyResource("/memory.stat", new File(memoryDir, "memory.stat"));
		TestUtils.copyResource("/memory.numa_stat", new File(memoryDir, "memory.numa_stat"));
	}

	@Test
	public void testMonitor() {
		final CgroupMemoryMonitor monitor = new CgroupMemoryMonitor(discoverer, ImmutableMap.of(), "some_feed");
		// final StubServiceEmitter emitter = new StubServiceEmitter("service",
		// "host");
		List<Event> events = new ArrayList<>();
		ServiceEmitter emitter = Mockito.mock(ServiceEmitter.class, Mockito.withSettings()
				.defaultAnswer(Mockito.CALLS_REAL_METHODS).useConstructor("service", "host", null));
		Mockito.doAnswer(invo -> {
			events.add(invo.getArgument(0));
			return null;
		}).when(emitter).emit(Mockito.any(Event.class));
		Mockito.doNothing().when(emitter).start();
		try {
			Mockito.doNothing().when(emitter).flush();
			Mockito.doNothing().when(emitter).close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		Assert.assertTrue(monitor.doMonitor(emitter));
		Assert.assertEquals(44, events.size());
	}
}
