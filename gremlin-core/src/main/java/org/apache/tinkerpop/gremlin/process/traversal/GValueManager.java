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

import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;

public class GValueManager {

    private Map<String, GValue> gValueRegistry = Collections.synchronizedMap(new IdentityHashMap());
    private Map<Step, StepRegistration> stepRegistry = Collections.synchronizedMap(new IdentityHashMap());
    private Map<Step, P> predicateRegistry = Collections.synchronizedMap(new IdentityHashMap());

    public void register(Step step, GValue... gValues) {
        for (GValue<?> gValue : gValues) {
            if (gValue != null && gValue.isVariable()) {
                if (gValueRegistry.containsKey(gValue.getName())) {
                    //TODO Handle better?
                    throw new IllegalArgumentException("Cannot register multiple values for " + gValue.getName());
                }
                gValueRegistry.put(gValue.getName(), gValue);
            }
        }
        stepRegistry.put(step, new StepRegistration(
                Arrays.stream(gValues).map((GValue gvalue) -> gvalue == null ? null : gvalue.getName()).toArray(String[]::new)
        ));
    }

    /**
     * Convenience overload for register which handles a mix of GValues and literals.
     * @param step
     * @param gValues
     */
    public void register(Step step, Object... gValues) {
        if (gValues == null || gValues.length == 0) {
            return; //nothing to register
        }
        register(step, Arrays.stream(gValues).map((arg) -> arg instanceof GValue ? arg : null).toArray(GValue<?>[]::new));
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

//    public List<String> getGvalueForStep(Step step){
//
//    }

    /**
     * Copy parameter state from `fromStep` to `toStep`
     */
    public <S, E> void copyParams(Step<S,E> fromStep, Step<S,E> toStep) {
        if (stepRegistry.containsKey(fromStep)) {
            try {
                stepRegistry.put(toStep, (StepRegistration)  stepRegistry.get(fromStep).clone());
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public <S> boolean isParameterized(Step step) {
        return this.stepRegistry.containsKey(step);
    }

    private class StepRegistration implements Cloneable {
        private List<String> params;

        public StepRegistration(String... name) {
            this.params = Arrays.stream(name).collect(Collectors.toList());
        }

        public List<String> getParams() {
            return params;
        }

        public Object clone() throws CloneNotSupportedException {
            //TODO deep copy
            StepRegistration clone = (StepRegistration) super.clone();
            return clone;
        }
    }
}
