/*
 * Copyright (c) 2020. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector.impl;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class AxonConnectorThreadFactory implements ThreadFactory {

    private final ThreadGroup threadGroup;
    private final AtomicInteger threadNumber = new AtomicInteger();

    private AxonConnectorThreadFactory(String instanceId) {
        threadGroup = new ThreadGroup("axon-connector-" + instanceId);
    }

    public static AxonConnectorThreadFactory forInstanceId(String instanceId) {
        return new AxonConnectorThreadFactory(instanceId);
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(threadGroup, r, threadGroup.getName() + "-" + nextThreadNumber());
    }

    private int nextThreadNumber() {
        return threadNumber.getAndIncrement();
    }
}