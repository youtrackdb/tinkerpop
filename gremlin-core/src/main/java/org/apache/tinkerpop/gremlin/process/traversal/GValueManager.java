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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.step.ConstantContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.ElementContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.EdgeLabelContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.RangeContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepContract;

import java.util.IdentityHashMap;
import java.util.Map;

public class GValueManager {

    private Map<String, GValue> gValueRegistry = new IdentityHashMap();
    private Map<Step, StepContract> stepRegistry = new IdentityHashMap();
    private Map<Step, P> predicateRegistry = new IdentityHashMap();

    /**
     * Register a step with a GValue RangeContract
     */
    public void register(Step step, RangeContract<GValue<Long>> contract) {
        stepRegistry.put(step, contract);
        if (contract.getHighRange().getName() != null) {
            gValueRegistry.put(contract.getHighRange().getName(), contract.getHighRange());
        }
        if (contract.getLowRange().getName() != null) {
            gValueRegistry.put(contract.getLowRange().getName(), contract.getLowRange());
        }
    }

    /**
     * Register a step with a GValue ConstantContract
     */
    public void register(Step step, ConstantContract<GValue> contract) {
        stepRegistry.put(step, contract);
        if (contract.getConstant().getName() != null) {
            gValueRegistry.put(contract.getConstant().getName(), contract.getConstant());
        }
    }

    /**
     * Register a step with a GValue ElementContract
     */
    public void register(Step step, ElementContract<GValue> contract) {
        stepRegistry.put(step, contract);
        for (GValue gValue: contract.getIds()) {
            if (gValue.getName() != null) {
                gValueRegistry.put(gValue.getName(), gValue);
            }
        }
    }

    /**
     * Register a step with a GValue LabelContract
     */
    public void register(Step step, EdgeLabelContract<GValue<String>> contract) {
        stepRegistry.put(step, contract);
        for (GValue gValue: contract.getEdgeLabels()) {
            if (gValue.getName() != null) {
                gValueRegistry.put(gValue.getName(), gValue);
            }
        }
    }

    public void register(Step step, P predicate) {
        predicateRegistry.put(step, predicate);
    }

    public void merge(GValueManager other) {
        //TODO deal with conflicts
        gValueRegistry.putAll(other.gValueRegistry);
        stepRegistry.putAll(other.stepRegistry);
        predicateRegistry.putAll(other.predicateRegistry);
    }

    /**
     * Copy parameter state from `fromStep` to `toStep`
     */
    public <S, E> void copyParams(Step<S,E> fromStep, Step<S,E> toStep) {
        if (stepRegistry.containsKey(fromStep)) {
            stepRegistry.put(toStep, stepRegistry.get(fromStep)); //TODO Deep Copy?
        }
    }

    public StepContract getStepContract(Step step) {
        return stepRegistry.get(step);
    }

    public <S> boolean isParameterized(Step step) {
        return this.stepRegistry.containsKey(step) || this.predicateRegistry.containsKey(step);
    }
}
