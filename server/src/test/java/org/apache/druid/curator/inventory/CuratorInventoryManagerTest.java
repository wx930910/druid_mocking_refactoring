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

package org.apache.druid.curator.inventory;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;

/**
 */
public class CuratorInventoryManagerTest extends CuratorTestBase {
	private ExecutorService exec;

	@Before
	public void setUp() throws Exception {
		setupServerAndCurator();
		exec = Execs.singleThreaded("curator-inventory-manager-test-%s");
	}

	@After
	public void tearDown() {
		tearDownServerAndCurator();
	}

	@Test
	public void testSanity() throws Exception {
		CountDownLatch[] newContainerLatch = new CountDownLatch[1];
		CountDownLatch[] deadContainerLatch = new CountDownLatch[1];
		CountDownLatch[] newInventoryLatch = new CountDownLatch[1];
		CountDownLatch[] deadInventoryLatch = new CountDownLatch[1];
		boolean[] initialized = { false };
		CuratorInventoryManagerStrategy<Map<String, Integer>, Integer> strategy = Mockito
				.mock(CuratorInventoryManagerStrategy.class);
		Mockito.doAnswer(invo -> {
			initialized[0] = true;
			return null;
		}).when(strategy).inventoryInitialized();
		Mockito.when(strategy.deserializeContainer(Mockito.any())).thenReturn(new TreeMap<>());
		Mockito.when(strategy.deserializeInventory(Mockito.any())).thenAnswer(invo -> {
			byte[] bytes = invo.getArgument(0);
			return Ints.fromByteArray(bytes);
		});
		Mockito.doAnswer(invo -> {
			if (newContainerLatch[0] != null) {
				newContainerLatch[0].countDown();
			}
			return null;
		}).when(strategy).newContainer(Mockito.anyMap());
		Mockito.doAnswer(invo -> {
			if (deadContainerLatch[0] != null) {
				deadContainerLatch[0].countDown();
			}
			return null;
		}).when(strategy).deadContainer(Mockito.anyMap());
		Mockito.when(strategy.updateContainer(Mockito.anyMap(), Mockito.anyMap())).thenAnswer(invo -> {
			Map<String, Integer> oldContainer = invo.getArgument(0);
			Map<String, Integer> newContainer = invo.getArgument(1);
			newContainer.putAll(oldContainer);
			return newContainer;
		});
		Mockito.when(strategy.addInventory(Mockito.anyMap(), Mockito.anyString(), Mockito.anyInt()))
				.thenAnswer(invo -> {
					Map<String, Integer> container = invo.getArgument(0);
					String inventoryKey = invo.getArgument(1);
					Integer inventory = invo.getArgument(2);
					container.put(inventoryKey, inventory);
					if (newInventoryLatch[0] != null) {
						newInventoryLatch[0].countDown();
					}
					return container;
				});
		Mockito.when(strategy.updateInventory(Mockito.anyMap(), Mockito.anyString(), Mockito.anyInt()))
				.thenAnswer(invo -> {
					Map<String, Integer> container = invo.getArgument(0);
					String inventoryKey = invo.getArgument(1);
					Integer inventory = invo.getArgument(2);
					return strategy.addInventory(container, inventoryKey, inventory);
				});
		Mockito.when(strategy.removeInventory(Mockito.anyMap(), Mockito.anyString())).thenAnswer(invo -> {
			Map<String, Integer> container = invo.getArgument(0);
			String inventoryKey = invo.getArgument(1);
			container.remove(inventoryKey);
			if (deadInventoryLatch[0] != null) {
				deadInventoryLatch[0].countDown();
			}
			return container;
		});
		// final MapStrategy strategy = new MapStrategy();
		CuratorInventoryManager<Map<String, Integer>, Integer> manager = new CuratorInventoryManager<>(curator,
				mockInventoryManagerConfig("/container", "/inventory"), exec, strategy);

		curator.start();
		curator.blockUntilConnected();

		manager.start();

		Assert.assertTrue(Iterables.isEmpty(manager.getInventory()));

		CountDownLatch containerLatch = new CountDownLatch(1);
		CountDownLatch inventoryLatch = new CountDownLatch(2);
		CountDownLatch deleteLatch = new CountDownLatch(1);

		newContainerLatch[0] = containerLatch;
		curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/container/billy",
				new byte[] {});

		Assert.assertTrue(timing.awaitLatch(containerLatch));
		newContainerLatch[0] = null;

		final Iterable<Map<String, Integer>> inventory = manager.getInventory();
		Assert.assertTrue(Iterables.getOnlyElement(inventory).isEmpty());

		newInventoryLatch[0] = inventoryLatch;
		curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/inventory/billy/1",
				Ints.toByteArray(100));
		curator.create().withMode(CreateMode.EPHEMERAL).forPath("/inventory/billy/bob", Ints.toByteArray(2287));

		Assert.assertTrue(timing.awaitLatch(inventoryLatch));
		newInventoryLatch[0] = null;

		verifyInventory(manager);

		deadInventoryLatch[0] = deleteLatch;
		curator.delete().forPath("/inventory/billy/1");

		Assert.assertTrue(timing.awaitLatch(deleteLatch));
		deadInventoryLatch[0] = null;

		Assert.assertEquals(1, manager.getInventoryValue("billy").size());
		Assert.assertEquals(2287, manager.getInventoryValue("billy").get("bob").intValue());

		inventoryLatch = new CountDownLatch(1);
		newInventoryLatch[0] = inventoryLatch;
		curator.create().withMode(CreateMode.EPHEMERAL).forPath("/inventory/billy/1", Ints.toByteArray(100));

		Assert.assertTrue(timing.awaitLatch(inventoryLatch));
		newInventoryLatch[0] = null;

		verifyInventory(manager);

		final CountDownLatch latch = new CountDownLatch(1);
		curator.getCuratorListenable().addListener(new CuratorListener() {
			@Override
			public void eventReceived(CuratorFramework client, CuratorEvent event) {
				if (event.getType() == CuratorEventType.WATCHED
						&& event.getWatchedEvent().getState() == Watcher.Event.KeeperState.Disconnected) {
					latch.countDown();
				}
			}
		});

		server.stop();
		Assert.assertTrue(timing.awaitLatch(latch));

		verifyInventory(manager);

		Thread.sleep(50); // Wait a bit

		verifyInventory(manager);
	}

	private void verifyInventory(CuratorInventoryManager<Map<String, Integer>, Integer> manager) {
		final Map<String, Integer> vals = manager.getInventoryValue("billy");
		Assert.assertEquals(2, vals.size());
		Assert.assertEquals(100, vals.get("1").intValue());
		Assert.assertEquals(2287, vals.get("bob").intValue());
	}

	private InventoryManagerConfig mockInventoryManagerConfig(String st1, String st2) {
		// final String[] containerPath = { st1 };
		// final String[] inventoryPath = { st2 };
		InventoryManagerConfig res = Mockito.mock(InventoryManagerConfig.class);
		Mockito.when(res.getContainerPath()).thenReturn(st1);
		Mockito.when(res.getInventoryPath()).thenReturn(st2);
		return res;
	}

	private static class StringInventoryManagerConfig implements InventoryManagerConfig {
		private final String containerPath;
		private final String inventoryPath;

		private StringInventoryManagerConfig(String containerPath, String inventoryPath) {
			this.containerPath = containerPath;
			this.inventoryPath = inventoryPath;
		}

		@Override
		public String getContainerPath() {
			return containerPath;
		}

		@Override
		public String getInventoryPath() {
			return inventoryPath;
		}
	}

	private static class MapStrategy implements CuratorInventoryManagerStrategy<Map<String, Integer>, Integer> {
		private volatile CountDownLatch newContainerLatch = null;
		private volatile CountDownLatch deadContainerLatch = null;
		private volatile CountDownLatch newInventoryLatch = null;
		private volatile CountDownLatch deadInventoryLatch = null;
		private volatile boolean initialized = false;

		@Override
		public Map<String, Integer> deserializeContainer(byte[] bytes) {
			return new TreeMap<>();
		}

		@Override
		public Integer deserializeInventory(byte[] bytes) {
			return Ints.fromByteArray(bytes);
		}

		@Override
		public void newContainer(Map<String, Integer> newContainer) {
			if (newContainerLatch != null) {
				newContainerLatch.countDown();
			}
		}

		@Override
		public void deadContainer(Map<String, Integer> deadContainer) {
			if (deadContainerLatch != null) {
				deadContainerLatch.countDown();
			}
		}

		@Override
		public Map<String, Integer> updateContainer(Map<String, Integer> oldContainer,
				Map<String, Integer> newContainer) {
			newContainer.putAll(oldContainer);
			return newContainer;
		}

		@Override
		public Map<String, Integer> addInventory(Map<String, Integer> container, String inventoryKey,
				Integer inventory) {
			container.put(inventoryKey, inventory);
			if (newInventoryLatch != null) {
				newInventoryLatch.countDown();
			}
			return container;
		}

		@Override
		public Map<String, Integer> updateInventory(Map<String, Integer> container, String inventoryKey,
				Integer inventory) {
			return addInventory(container, inventoryKey, inventory);
		}

		@Override
		public Map<String, Integer> removeInventory(Map<String, Integer> container, String inventoryKey) {
			container.remove(inventoryKey);
			if (deadInventoryLatch != null) {
				deadInventoryLatch.countDown();
			}
			return container;
		}

		private void setNewContainerLatch(CountDownLatch newContainerLatch) {
			this.newContainerLatch = newContainerLatch;
		}

		private void setDeadContainerLatch(CountDownLatch deadContainerLatch) {
			this.deadContainerLatch = deadContainerLatch;
		}

		private void setNewInventoryLatch(CountDownLatch newInventoryLatch) {
			this.newInventoryLatch = newInventoryLatch;
		}

		private void setDeadInventoryLatch(CountDownLatch deadInventoryLatch) {
			this.deadInventoryLatch = deadInventoryLatch;
		}

		@Override
		public void inventoryInitialized() {
			initialized = true;
		}
	}
}
