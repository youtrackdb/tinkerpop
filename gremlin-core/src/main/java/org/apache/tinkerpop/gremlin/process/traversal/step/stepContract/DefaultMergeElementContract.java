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

public class DefaultMergeElementContract<V> implements MergeElementContract<V> {
    private V mergeMap;
    private V onCreateMap;
    private V onMatchMap;

    public DefaultMergeElementContract(V mergeMap) {
        this.mergeMap = mergeMap;
    }

    @Override
    public void setMergeMap(V mergeMap) {
        this.mergeMap = mergeMap;
    }

    @Override
    public void setOnCreateMap(V onCreateMap) {
        this.onCreateMap = onCreateMap;
    }

    @Override
    public void setOnMatchMap(V onMatchMap) {
        this.onMatchMap = onMatchMap;
    }

    @Override
    public V getMergeMap() {
        return mergeMap;
    }

    @Override
    public V getOnCreateMap() {
        return onCreateMap;
    }

    @Override
    public V getOnMatchMap() {
        return onMatchMap;
    }
}
