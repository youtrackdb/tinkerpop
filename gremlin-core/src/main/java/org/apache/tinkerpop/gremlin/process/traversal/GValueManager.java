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

import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddEdgeContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddVertexContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.CallContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.ElementIdsContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.EdgeLabelContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddPropertyContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.MergeElementContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.RangeContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.StepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.TailContract;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.IdentityHashMap;
import java.util.Map;

public class GValueManager implements Serializable {

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
     * Register a step with a GValue TailContract
     */
    public void register(Step step, TailContract<GValue<Long>> contract) {
        stepRegistry.put(step, contract);
        if (contract.getLimit().getName() != null) {
            gValueRegistry.put(contract.getLimit().getName(), contract.getLimit());
        }
    }

    /**
     * Register a step with a GValue MergeElementContract
     */
    public void register(Step step, MergeElementContract<GValue<Map<?,?>>> contract) {
        stepRegistry.put(step, contract);
        if (contract.getMergeMap() != null) {
            gValueRegistry.put(contract.getMergeMap().getName(), contract.getMergeMap());
        }
        if (contract.getOnCreateMap() != null) {
            gValueRegistry.put(contract.getOnCreateMap().getName(), contract.getOnCreateMap());
        }
        if (contract.getOnMatchMap() != null) {
            gValueRegistry.put(contract.getOnMatchMap().getName(), contract.getOnMatchMap());
        }
    }

    /**
     * Register a step with a GValue ElementContract
     */
    public void register(Step step, ElementIdsContract<GValue> contract) {
        stepRegistry.put(step, contract);
        for (GValue gValue: contract.getIds()) {
            if (gValue.getName() != null) {
                gValueRegistry.put(gValue.getName(), gValue);
            }
        }
    }

    /**
     * Register a step with a GValue AddVertexContract
     */
    public void register(Step step, AddVertexContract<GValue<String>, ?, GValue<?>> contract) {
        stepRegistry.put(step, contract);
        gValueRegistry.put(contract.getLabel().getName(), contract.getLabel());
        for (GValue<?> value : contract.getProperties().values()) {
            gValueRegistry.put(value.getName(), value);
        }
    }

    /**
     * Register a step with a GValue AddEdgeContract
     */
    public void register(Step step, AddEdgeContract<GValue<String>, GValue<Vertex>, ?, GValue<?>> contract) {
        stepRegistry.put(step, contract);
        gValueRegistry.put(contract.getLabel().getName(), contract.getLabel());
        if (contract.getFrom() != null) {
            gValueRegistry.put(contract.getFrom().getName(), contract.getFrom());

        }
        if (contract.getTo() != null) {
            gValueRegistry.put(contract.getTo().getName(), contract.getTo());
        }
        if (contract.getProperties() != null) {
            for (GValue<?> value : contract.getProperties().values()) {
                gValueRegistry.put(value.getName(), value);
            }
        }
    }

    /**
     * Register a step with a GValue AddPropertyContract
     */
    public void register(Step step, AddPropertyContract<?, GValue<?>> contract) {
        stepRegistry.put(step, contract);
        gValueRegistry.put(contract.getValue().getName(), contract.getValue());
        for (GValue<?> value : contract.getProperties().values()) {
            gValueRegistry.put(value.getName(), value);
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

    /**
     * Register a step with a GValue CallContract
     */
    public void register(Step step, CallContract<GValue<Map<?,?>>> contract) {
        stepRegistry.put(step, contract);
        if (contract.getStaticParams().getName() != null) {
            gValueRegistry.put(contract.getStaticParams().getName(), contract.getStaticParams());
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


    /**
     * Delete all data
     */
    public void reset() {
        stepRegistry.clear();
        gValueRegistry.clear();
    }

    /**
     * Delete all data for all GValues associated with a single Step
     * @param step
     */
    public void reset(Step step) {
        StepContract contract = stepRegistry.remove(step);

        if (contract != null) {
            if(contract instanceof RangeContract){
                removeFromGValueRegistry(((RangeContract<GValue<?>>) contract).getHighRange());
                removeFromGValueRegistry(((RangeContract<GValue<?>>) contract).getLowRange());
            }// TODO else if chain for all contract types
        }

    }

    private void removeFromGValueRegistry(GValue<?> gValue) {
        if (gValue == null || gValue.getName() == null) {
            return;
        }
        gValueRegistry.remove(gValue.getName());
        //TODO:: Find if any steps are using same GValue and remove.
    }
}
