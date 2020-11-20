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

package org.apache.druid.server.log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.druid.server.RequestLogLine;
import org.mockito.Mockito;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("test")
public class TestRequestLoggerProvider implements RequestLoggerProvider {
	@Override
	public RequestLogger get() {
		final List<RequestLogLine> nativeQuerylogs = new ArrayList<>();
		final List<RequestLogLine> sqlQueryLogs = new ArrayList<>();
		final AtomicBoolean started = new AtomicBoolean();
		RequestLogger res = Mockito.mock(RequestLogger.class);
		try {
			Mockito.doAnswer(invo -> {
				started.set(true);
				return null;
			}).when(res).start();
			Mockito.doAnswer(invo -> {
				started.set(false);
				return null;
			}).when(res).stop();
			Mockito.doAnswer(invo -> {
				final RequestLogLine requestLogLine = invo.getArgument(0);
				synchronized (nativeQuerylogs) {
					nativeQuerylogs.add(requestLogLine);
				}
				return null;
			}).when(res).logNativeQuery(Mockito.any());
			Mockito.doAnswer(invo -> {
				RequestLogLine requestLogLine = invo.getArgument(0);
				synchronized (sqlQueryLogs) {
					sqlQueryLogs.add(requestLogLine);
				}
				return null;
			}).when(res).logNativeQuery(Mockito.any());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new TestRequestLogger();
	}
}
