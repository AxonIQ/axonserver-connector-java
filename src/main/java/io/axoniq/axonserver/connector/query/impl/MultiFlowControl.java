/*
 * Copyright (c) 2020-2021. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector.query.impl;

import io.axoniq.axonserver.connector.query.QueryHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of {@link QueryHandler.FlowControl} that delegates operations to several registered {@link
 * QueryHandler.FlowControl} implementations.
 */
public class MultiFlowControl implements QueryHandler.FlowControl {

    private final List<QueryHandler.FlowControl> delegates;
    private final AtomicInteger discriminator = new AtomicInteger();

    /**
     * Creates an instance of {@link MultiFlowControl} with no delegates.
     */
    public MultiFlowControl() {
        this(Collections.emptyList());
    }

    /**
     * Creates an instance of {@link MultiFlowControl} with given {@code delegates}.
     *
     * @param delegates flow control delegates
     */
    public MultiFlowControl(QueryHandler.FlowControl... delegates) {
        this(Arrays.asList(delegates));
    }

    /**
     * Creates an instance of {@link MultiFlowControl} with given {@code delegates}.
     *
     * @param delegates flow control delegates
     */
    public MultiFlowControl(List<QueryHandler.FlowControl> delegates) {
        this.delegates = new ArrayList<>(flatten(delegates));
    }

    private static List<QueryHandler.FlowControl> flatten(List<QueryHandler.FlowControl> flowControlList) {
        List<QueryHandler.FlowControl> flattened = new ArrayList<>(flowControlList.size());
        for (QueryHandler.FlowControl flowControl : flowControlList) {
            if (flowControl instanceof MultiFlowControl) {
                flattened.addAll(((MultiFlowControl) flowControl).delegates());
            } else {
                flattened.add(flowControl);
            }
        }
        return flattened;
    }

    /**
     * Creates a new instance of {@link MultiFlowControl} with given {@code flowControl} added.
     *
     * @param flowControl the {@link QueryHandler.FlowControl} implementation to be registered as a delegate
     * @return a new instance of {@link MultiFlowControl}
     */
    public MultiFlowControl with(QueryHandler.FlowControl flowControl) {
        ArrayList<QueryHandler.FlowControl> flowControlList = new ArrayList<>(delegates);
        flowControlList.add(flowControl);
        return new MultiFlowControl(flowControlList);
    }

    /**
     * Returns a {@link Collections#unmodifiableList(List)} of {@link QueryHandler.FlowControl} delegates.
     *
     * @return {@link Collections#unmodifiableList(List)} of {@link QueryHandler.FlowControl} delegates
     */
    public List<QueryHandler.FlowControl> delegates() {
        return Collections.unmodifiableList(delegates);
    }

    @Override
    public void request(long requested) {
        int size = delegates.size();
        if (size == 0) {
            return;
        }
        int index = discriminator.getAndIncrement() % size;
        delegates.get(index)
                 .request(requested);
    }

    @Override
    public void cancel() {
        delegates.forEach(QueryHandler.FlowControl::cancel);
    }
}
