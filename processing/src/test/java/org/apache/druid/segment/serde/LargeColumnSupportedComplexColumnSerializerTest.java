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

package org.apache.druid.segment.serde;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.Smoosh;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import it.unimi.dsi.fastutil.bytes.ByteArrays;

public class LargeColumnSupportedComplexColumnSerializerTest {

	private final HashFunction fn = Hashing.murmur3_128();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testSanity() throws IOException {
		HashFunction hashFn = Hashing.murmur3_128();
		Comparator<HyperLogLogCollector> comparator = Comparator
				.nullsFirst(Comparator.comparing(HyperLogLogCollector::toByteBuffer));
		ComplexMetricSerde serde = Mockito.mock(ComplexMetricSerde.class, Mockito.CALLS_REAL_METHODS);
		Mockito.when(serde.getTypeName()).thenReturn("hyperUnique");
		Mockito.doAnswer(invo -> {
			return new ComplexMetricExtractor() {
				@Override
				public Class<HyperLogLogCollector> extractedClass() {
					return HyperLogLogCollector.class;
				}

				@Override
				public HyperLogLogCollector extractValue(InputRow inputRow, String metricName) {
					Object rawValue = inputRow.getRaw(metricName);

					if (rawValue instanceof HyperLogLogCollector) {
						return (HyperLogLogCollector) rawValue;
					} else {
						HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

						List<String> dimValues = inputRow.getDimension(metricName);
						if (dimValues == null) {
							return collector;
						}

						for (String dimensionValue : dimValues) {
							collector.add(hashFn.hashBytes(StringUtils.toUtf8(dimensionValue)).asBytes());
						}
						return collector;
					}
				}
			};
		}).when(serde).getExtractor();
		Mockito.doAnswer(invo -> {
			ByteBuffer byteBuffer = invo.getArgument(0);
			ColumnBuilder columnBuilder = invo.getArgument(1);
			final GenericIndexed column;
			if (columnBuilder.getFileMapper() == null) {
				column = GenericIndexed.read(byteBuffer, serde.getObjectStrategy());
			} else {
				column = GenericIndexed.read(byteBuffer, serde.getObjectStrategy(), columnBuilder.getFileMapper());
			}

			columnBuilder.setComplexColumnSupplier(new ComplexColumnPartSupplier(serde.getTypeName(), column));
			return null;
		}).when(serde).deserializeColumn(Mockito.any(), Mockito.any());
		Mockito.when(serde.getObjectStrategy()).thenAnswer(invo -> {
			return new ObjectStrategy<HyperLogLogCollector>() {
				@Override
				public Class<? extends HyperLogLogCollector> getClazz() {
					return HyperLogLogCollector.class;
				}

				@Override
				public HyperLogLogCollector fromByteBuffer(ByteBuffer buffer, int numBytes) {
					final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
					readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
					return HyperLogLogCollector.makeCollector(readOnlyBuffer);
				}

				@Override
				public byte[] toBytes(HyperLogLogCollector collector) {
					if (collector == null) {
						return ByteArrays.EMPTY_ARRAY;
					}
					ByteBuffer val = collector.toByteBuffer();
					byte[] retVal = new byte[val.remaining()];
					val.asReadOnlyBuffer().get(retVal);
					return retVal;
				}

				@Override
				public int compare(HyperLogLogCollector o1, HyperLogLogCollector o2) {
					return comparator.compare(o1, o2);
				}
			};
		});
		Mockito.when(serde.getSerializer(Mockito.any(), Mockito.anyString())).thenAnswer(invo -> {
			SegmentWriteOutMedium segmentWriteOutMedium = invo.getArgument(0);
			String metric = invo.getArgument(1);
			return LargeColumnSupportedComplexColumnSerializer.createWithColumnSize(segmentWriteOutMedium, metric,
					serde.getObjectStrategy(), Integer.MAX_VALUE);
		});
		// HyperUniquesSerdeForTest serde = new
		// HyperUniquesSerdeForTest(Hashing.murmur3_128());
		int[] cases = { 1000, 5000, 10000, 20000 };
		int[] columnSizes = { Integer.MAX_VALUE, Integer.MAX_VALUE / 2, Integer.MAX_VALUE / 4, 5000 * Long.BYTES,
				2500 * Long.BYTES };

		for (int columnSize : columnSizes) {
			for (int aCase : cases) {
				File tmpFile = temporaryFolder.newFolder();
				HyperLogLogCollector baseCollector = HyperLogLogCollector.makeLatestCollector();
				try (SegmentWriteOutMedium segmentWriteOutMedium = new OffHeapMemorySegmentWriteOutMedium();
						FileSmoosher v9Smoosher = new FileSmoosher(tmpFile)) {

					LargeColumnSupportedComplexColumnSerializer serializer = LargeColumnSupportedComplexColumnSerializer
							.createWithColumnSize(segmentWriteOutMedium, "test", serde.getObjectStrategy(), columnSize);

					serializer.open();
					for (int i = 0; i < aCase; i++) {
						HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
						byte[] hashBytes = fn.hashLong(i).asBytes();
						collector.add(hashBytes);
						baseCollector.fold(collector);
						serializer.serialize(new ObjectColumnSelector() {
							@Nullable
							@Override
							public Object getObject() {
								return collector;
							}

							@Override
							public Class classOfObject() {
								return HyperLogLogCollector.class;
							}

							@Override
							public void inspectRuntimeShape(RuntimeShapeInspector inspector) {
								// doesn't matter in tests
							}
						});
					}

					try (final SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter("test",
							serializer.getSerializedSize())) {
						serializer.writeTo(channel, v9Smoosher);
					}
				}

				SmooshedFileMapper mapper = Smoosh.map(tmpFile);
				final ColumnBuilder builder = new ColumnBuilder().setType(ValueType.COMPLEX).setHasMultipleValues(false)
						.setFileMapper(mapper);
				serde.deserializeColumn(mapper.mapFile("test"), builder);

				ColumnHolder columnHolder = builder.build();
				ComplexColumn complexColumn = (ComplexColumn) columnHolder.getColumn();
				HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

				for (int i = 0; i < aCase; i++) {
					collector.fold((HyperLogLogCollector) complexColumn.getRowValue(i));
				}
				Assert.assertEquals(baseCollector.estimateCardinality(), collector.estimateCardinality(), 0.0);
			}
		}
	}

}
