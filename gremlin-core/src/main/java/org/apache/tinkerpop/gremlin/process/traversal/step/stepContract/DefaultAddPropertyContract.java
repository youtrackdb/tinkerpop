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

import java.util.HashMap;
import java.util.Map;

public class DefaultAddPropertyContract<K, V> implements AddPropertyContract<K, V> {
    private final K key;
    private final V value;
    private final Map<K, V> metaProperties = new HashMap<>();

    public DefaultAddPropertyContract(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public void addProperty(K key, V value){
        metaProperties.put(key, value);
    }

    @Override
    public Map<K, V> getProperties() {
        throw new UnsupportedOperationException();
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }
}
