/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step.stepContract;

import org.apache.commons.lang3.NotImplementedException;

import java.util.Map;

public class DefaultAddEdgeContract<L, Vertex, K, Value> implements AddEdgeContract<L, Vertex, K, Value> {
    private L label;
    private Vertex from;
    private Vertex to;
    private Map<K, Value> properties;

    public DefaultAddEdgeContract(L label) {
        this.label = label;
    }

    @Override
    public L getLabel() {
        return label;
    }

    @Override
    public Vertex getFrom() {
        return null;
    }

    @Override
    public Vertex getTo() {
        return null;
    }

    public void addFrom(Vertex from) {
        this.from = from;
    }

    public void addTo(Vertex to) {
        this.to = to;
    }

    @Override
    public Map<K, Value> getProperties() {
        return properties;
    }

    public void addProperty(K key, Value value){
        properties.put(key, value);
    }
}
