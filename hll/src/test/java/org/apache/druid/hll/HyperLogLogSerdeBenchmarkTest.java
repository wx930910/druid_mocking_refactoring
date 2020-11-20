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

package org.apache.druid.hll;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * TODO rewrite to use JMH and move to the benchmarks project
 */
@RunWith(Parameterized.class)
// @Ignore // Don't need to run every time
public class HyperLogLogSerdeBenchmarkTest extends AbstractBenchmark {
	private final HyperLogLogCollector collector;
	private final long NUM_HASHES;

	public HyperLogLogSerdeBenchmarkTest(final HyperLogLogCollector collector, Long num_hashes) {
		this.collector = collector;
		this.NUM_HASHES = num_hashes;
	}

	private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

	@Parameterized.Parameters
	public static Collection<Object[]> getParameters() {
		VersionOneHyperLogLogCollector prior = Mockito.spy(VersionOneHyperLogLogCollector.class);
		Mockito.when(prior.toByteArray()).thenAnswer(inv -> {
			final ByteBuffer myBuffer = prior.getStorageBuffer();
			final int initialPosition = prior.getInitPosition();
			short numNonZeroRegisters = prior.getNumNonZeroRegisters();

			// store sparsely
			if (myBuffer.remaining() == prior.getNumBytesForDenseStorage()
					&& numNonZeroRegisters < VersionOneHyperLogLogCollector.DENSE_THRESHOLD) {
				ByteBuffer retVal = ByteBuffer.wrap(new byte[numNonZeroRegisters * 3 + prior.getNumHeaderBytes()]);
				prior.setVersion(retVal);
				prior.setRegisterOffset(retVal, prior.getRegisterOffset());
				prior.setNumNonZeroRegisters(retVal, numNonZeroRegisters);
				prior.setMaxOverflowValue(retVal, prior.getMaxOverflowValue());
				prior.setMaxOverflowRegister(retVal, prior.getMaxOverflowRegister());

				int startPosition = prior.getPayloadBytePosition();
				retVal.position(prior.getPayloadBytePosition(retVal));
				for (int i = startPosition; i < startPosition
						+ VersionOneHyperLogLogCollector.NUM_BYTES_FOR_BUCKETS; i++) {
					if (myBuffer.get(i) != 0) {
						retVal.putShort((short) (0xffff & (i - initialPosition)));
						retVal.put(myBuffer.get(i));
					}
				}
				retVal.rewind();
				return retVal.asReadOnlyBuffer();
			}

			return myBuffer.asReadOnlyBuffer();
		});
		return ImmutableList.of((Object[]) Arrays.asList(mockpriorByteBufferSerializer(), new Long(1 << 10)).toArray(),
				(Object[]) Arrays.asList(mocknewByteBufferSerializer(), new Long(1 << 10)).toArray(),
				(Object[]) Arrays.asList(new newByteBufferSerializerWithPuts(), new Long(1 << 10)).toArray(),
				(Object[]) Arrays.asList(prior, new Long(1 << 8)).toArray(),
				(Object[]) Arrays.asList(new newByteBufferSerializer(), new Long(1 << 8)).toArray(),
				(Object[]) Arrays.asList(new newByteBufferSerializerWithPuts(), new Long(1 << 8)).toArray(),
				(Object[]) Arrays.asList(new priorByteBufferSerializer(), new Long(1 << 5)).toArray(),
				(Object[]) Arrays.asList(new newByteBufferSerializer(), new Long(1 << 5)).toArray(),
				(Object[]) Arrays.asList(new newByteBufferSerializerWithPuts(), new Long(1 << 5)).toArray(),
				(Object[]) Arrays.asList(new priorByteBufferSerializer(), new Long(1 << 2)).toArray(),
				(Object[]) Arrays.asList(new newByteBufferSerializer(), new Long(1 << 2)).toArray(),
				(Object[]) Arrays.asList(new newByteBufferSerializerWithPuts(), new Long(1 << 2)).toArray());
	}

	private static VersionOneHyperLogLogCollector mocknewByteBufferSerializer() {
		VersionOneHyperLogLogCollector res = Mockito.spy(new VersionOneHyperLogLogCollector());
		Mockito.doAnswer(invo -> {
			final ByteBuffer myBuffer = res.getStorageBuffer();
			final int initialPosition = res.getInitPosition();
			final short numNonZeroRegisters = res.getNumNonZeroRegisters();

			// store sparsely
			if (myBuffer.remaining() == res.getNumBytesForDenseStorage() && numNonZeroRegisters < res.DENSE_THRESHOLD) {
				final ByteBuffer retVal = ByteBuffer.wrap(new byte[numNonZeroRegisters * 3 + res.getNumHeaderBytes()]);
				res.setVersion(retVal);
				res.setRegisterOffset(retVal, res.getRegisterOffset());
				res.setNumNonZeroRegisters(retVal, numNonZeroRegisters);
				res.setMaxOverflowValue(retVal, res.getMaxOverflowValue());
				res.setMaxOverflowRegister(retVal, res.getMaxOverflowRegister());

				final int startPosition = res.getPayloadBytePosition();
				retVal.position(res.getPayloadBytePosition(retVal));

				final byte[] zipperBuffer = new byte[res.NUM_BYTES_FOR_BUCKETS];
				ByteBuffer roStorageBuffer = myBuffer.asReadOnlyBuffer();
				roStorageBuffer.position(startPosition);
				roStorageBuffer.get(zipperBuffer);

				final ByteOrder byteOrder = retVal.order();

				final byte[] tempBuffer = new byte[numNonZeroRegisters * 3];
				int outBufferPos = 0;
				for (int i = 0; i < res.NUM_BYTES_FOR_BUCKETS; ++i) {
					if (zipperBuffer[i] != 0) {
						final short val = (short) (0xffff & (i + startPosition - initialPosition));
						if (byteOrder.equals(ByteOrder.LITTLE_ENDIAN)) {
							tempBuffer[outBufferPos + 0] = (byte) (0xff & val);
							tempBuffer[outBufferPos + 1] = (byte) (0xff & (val >> 8));
						} else {
							tempBuffer[outBufferPos + 1] = (byte) (0xff & val);
							tempBuffer[outBufferPos + 0] = (byte) (0xff & (val >> 8));
						}
						tempBuffer[outBufferPos + 2] = zipperBuffer[i];
						outBufferPos += 3;
					}
				}
				retVal.put(tempBuffer);
				retVal.rewind();
				return retVal.asReadOnlyBuffer();
			}

			return myBuffer.asReadOnlyBuffer();
		}).when(res).toByteBuffer();
		return res;
	}

	private static VersionOneHyperLogLogCollector mockpriorByteBufferSerializer() {
		VersionOneHyperLogLogCollector res = Mockito.spy(new VersionOneHyperLogLogCollector());
		Mockito.when(res.toByteBuffer()).thenAnswer(invo -> {
			final ByteBuffer myBuffer = res.getStorageBuffer();
			final int initialPosition = res.getInitPosition();
			short numNonZeroRegisters = res.getNumNonZeroRegisters();

			// store sparsely
			if (myBuffer.remaining() == res.getNumBytesForDenseStorage() && numNonZeroRegisters < res.DENSE_THRESHOLD) {
				ByteBuffer retVal = ByteBuffer.wrap(new byte[numNonZeroRegisters * 3 + res.getNumHeaderBytes()]);
				res.setVersion(retVal);
				res.setRegisterOffset(retVal, res.getRegisterOffset());
				res.setNumNonZeroRegisters(retVal, numNonZeroRegisters);
				res.setMaxOverflowValue(retVal, res.getMaxOverflowValue());
				res.setMaxOverflowRegister(retVal, res.getMaxOverflowRegister());

				int startPosition = res.getPayloadBytePosition();
				retVal.position(res.getPayloadBytePosition(retVal));
				for (int i = startPosition; i < startPosition + res.NUM_BYTES_FOR_BUCKETS; i++) {
					if (myBuffer.get(i) != 0) {
						retVal.putShort((short) (0xffff & (i - initialPosition)));
						retVal.put(myBuffer.get(i));
					}
				}
				retVal.rewind();
				return retVal.asReadOnlyBuffer();
			}

			return myBuffer.asReadOnlyBuffer();
		});
		return res;
	}

	private static final class priorByteBufferSerializer extends VersionOneHyperLogLogCollector {
		@Override
		public ByteBuffer toByteBuffer() {
			final ByteBuffer myBuffer = getStorageBuffer();
			final int initialPosition = getInitPosition();
			short numNonZeroRegisters = getNumNonZeroRegisters();

			// store sparsely
			if (myBuffer.remaining() == getNumBytesForDenseStorage() && numNonZeroRegisters < DENSE_THRESHOLD) {
				ByteBuffer retVal = ByteBuffer.wrap(new byte[numNonZeroRegisters * 3 + getNumHeaderBytes()]);
				setVersion(retVal);
				setRegisterOffset(retVal, getRegisterOffset());
				setNumNonZeroRegisters(retVal, numNonZeroRegisters);
				setMaxOverflowValue(retVal, getMaxOverflowValue());
				setMaxOverflowRegister(retVal, getMaxOverflowRegister());

				int startPosition = getPayloadBytePosition();
				retVal.position(getPayloadBytePosition(retVal));
				for (int i = startPosition; i < startPosition + NUM_BYTES_FOR_BUCKETS; i++) {
					if (myBuffer.get(i) != 0) {
						retVal.putShort((short) (0xffff & (i - initialPosition)));
						retVal.put(myBuffer.get(i));
					}
				}
				retVal.rewind();
				return retVal.asReadOnlyBuffer();
			}

			return myBuffer.asReadOnlyBuffer();
		}
	}

	private static final class newByteBufferSerializer extends VersionOneHyperLogLogCollector {
		@Override
		public ByteBuffer toByteBuffer() {

			final ByteBuffer myBuffer = getStorageBuffer();
			final int initialPosition = getInitPosition();
			final short numNonZeroRegisters = getNumNonZeroRegisters();

			// store sparsely
			if (myBuffer.remaining() == getNumBytesForDenseStorage() && numNonZeroRegisters < DENSE_THRESHOLD) {
				final ByteBuffer retVal = ByteBuffer.wrap(new byte[numNonZeroRegisters * 3 + getNumHeaderBytes()]);
				setVersion(retVal);
				setRegisterOffset(retVal, getRegisterOffset());
				setNumNonZeroRegisters(retVal, numNonZeroRegisters);
				setMaxOverflowValue(retVal, getMaxOverflowValue());
				setMaxOverflowRegister(retVal, getMaxOverflowRegister());

				final int startPosition = getPayloadBytePosition();
				retVal.position(getPayloadBytePosition(retVal));

				final byte[] zipperBuffer = new byte[NUM_BYTES_FOR_BUCKETS];
				ByteBuffer roStorageBuffer = myBuffer.asReadOnlyBuffer();
				roStorageBuffer.position(startPosition);
				roStorageBuffer.get(zipperBuffer);

				final ByteOrder byteOrder = retVal.order();

				final byte[] tempBuffer = new byte[numNonZeroRegisters * 3];
				int outBufferPos = 0;
				for (int i = 0; i < NUM_BYTES_FOR_BUCKETS; ++i) {
					if (zipperBuffer[i] != 0) {
						final short val = (short) (0xffff & (i + startPosition - initialPosition));
						if (byteOrder.equals(ByteOrder.LITTLE_ENDIAN)) {
							tempBuffer[outBufferPos + 0] = (byte) (0xff & val);
							tempBuffer[outBufferPos + 1] = (byte) (0xff & (val >> 8));
						} else {
							tempBuffer[outBufferPos + 1] = (byte) (0xff & val);
							tempBuffer[outBufferPos + 0] = (byte) (0xff & (val >> 8));
						}
						tempBuffer[outBufferPos + 2] = zipperBuffer[i];
						outBufferPos += 3;
					}
				}
				retVal.put(tempBuffer);
				retVal.rewind();
				return retVal.asReadOnlyBuffer();
			}

			return myBuffer.asReadOnlyBuffer();
		}
	}

	private static final class newByteBufferSerializerWithPuts extends VersionOneHyperLogLogCollector {
		@Override
		public ByteBuffer toByteBuffer() {
			final ByteBuffer myBuffer = getStorageBuffer();
			final int initialPosition = getInitPosition();

			final short numNonZeroRegisters = getNumNonZeroRegisters();

			// store sparsely
			if (myBuffer.remaining() == getNumBytesForDenseStorage() && numNonZeroRegisters < DENSE_THRESHOLD) {
				final ByteBuffer retVal = ByteBuffer.wrap(new byte[numNonZeroRegisters * 3 + getNumHeaderBytes()]);
				setVersion(retVal);
				setRegisterOffset(retVal, getRegisterOffset());
				setNumNonZeroRegisters(retVal, numNonZeroRegisters);
				setMaxOverflowValue(retVal, getMaxOverflowValue());
				setMaxOverflowRegister(retVal, getMaxOverflowRegister());

				final int startPosition = getPayloadBytePosition();
				retVal.position(getPayloadBytePosition(retVal));

				final byte[] zipperBuffer = new byte[NUM_BYTES_FOR_BUCKETS];
				ByteBuffer roStorageBuffer = myBuffer.asReadOnlyBuffer();
				roStorageBuffer.position(startPosition);
				roStorageBuffer.get(zipperBuffer);

				for (int i = 0; i < NUM_BYTES_FOR_BUCKETS; ++i) {
					if (zipperBuffer[i] != 0) {
						final short val = (short) (0xffff & (i + startPosition - initialPosition));
						retVal.putShort(val);
						retVal.put(zipperBuffer[i]);
					}
				}
				retVal.rewind();
				return retVal.asReadOnlyBuffer();
			}

			return myBuffer.asReadOnlyBuffer();
		}
	}

	// --------------------------------------------------------------------------------------------------------------------

	private void fillCollector(HyperLogLogCollector collector) {
		Random rand = new Random(758190);
		for (long i = 0; i < NUM_HASHES; ++i) {
			collector.add(HASH_FUNCTION.hashLong(rand.nextLong()).asBytes());
		}
	}

	private static HashCode getHash(final ByteBuffer byteBuffer) {
		Hasher hasher = HASH_FUNCTION.newHasher();
		while (byteBuffer.position() < byteBuffer.limit()) {
			hasher.putByte(byteBuffer.get());
		}
		return hasher.hash();
	}

	@BeforeClass
	public static void setupHash() {

	}

	@Before
	public void setup() {
		fillCollector(collector);
	}

	@SuppressWarnings("unused")
	volatile HashCode hashCode;

	@BenchmarkOptions(benchmarkRounds = 100000, warmupRounds = 100)
	@Test
	public void benchmarkToByteBuffer() {
		hashCode = getHash(collector.toByteBuffer());
	}
}
