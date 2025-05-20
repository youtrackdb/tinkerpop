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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The {@code GValueManager} class is responsible for managing the state of {@link GValue} instances and their
 * associations with `Step` objects in a traversal. This class ensures that `GValue` instances are properly extracted
 * and stored in a registry, allowing for  dynamic query optimizations and state management during traversal execution.
 * Note that the manager can be locked, at which point it becomes immutable, and any attempt to modify it will result
 * in an exception.
 */
public final class GValueManager implements Serializable {

    private boolean locked = false;
    private final Map<String, GValue> gValueRegistry = new IdentityHashMap();
    private final Map<Step, StepContract> stepRegistry = new IdentityHashMap();
    private final Map<Step, P> predicateRegistry = new IdentityHashMap();

    /**
     * Register a step with any {@link StepContract}.
     */
    public void register(final Step step, final StepContract contract) {
        if (this.locked) throw Traversal.Exceptions.traversalIsLocked();
        stepRegistry.put(step, contract);
    }

    /**
     * Register a step with any {@link P} containing a {@link GValue}.
     */
    public void register(final Step step, final P predicate) {
        if (this.locked) throw Traversal.Exceptions.traversalIsLocked();
        predicateRegistry.put(step, predicate);
    }

    /**
     * Locks the current state of the manager, ensuring that no further modifications can be made.
     * <p/>
     * This method processes all registered steps and predicates by extracting relevant {@link GValue}
     * instances from their associated contracts or predicates and adding them to the internal registry.
     * If the manager is already locked, invoking this method has no effect.
     * <p/>
     * During the locking process, the manager iterates over the registered steps and applies the appropriate
     * extraction logic for each contract type. If a contract type is found that is not supported, an
     * {@link IllegalArgumentException} is thrown.
     */
    public void lock() {
        // can only lock once so if already locked, just return.
        if (locked) return;

        for (Map.Entry<Step, StepContract> entry : stepRegistry.entrySet()) {
            final StepContract contract = entry.getValue();
            if (contract instanceof RangeContract) {
                extractGValue((RangeContract<GValue<Long>>) contract);
            } else if (contract instanceof TailContract) {
                extractGValue((TailContract<GValue<Long>>) contract);
            } else if (contract instanceof MergeElementContract) {
                extractGValue((MergeElementContract<GValue<Map<?, ?>>> ) contract);
            } else if (contract instanceof ElementIdsContract) {
                extractGValue((ElementIdsContract<GValue>) contract);
            } else if (contract instanceof AddVertexContract) {
                extractGValue((AddVertexContract<GValue<String>, ?, GValue<?>>) contract);
            } else if (contract instanceof AddEdgeContract) {
                extractGValue((AddEdgeContract<GValue<String>, GValue<Vertex>, ?, GValue<?>>) contract);
            } else if (contract instanceof AddPropertyContract) {
                extractGValue((AddPropertyContract<?, GValue<?>>) contract);
            } else if (contract instanceof EdgeLabelContract) {
                extractGValue((EdgeLabelContract<GValue<String>>) contract);
            } else if (contract instanceof CallContract) {
                extractGValue((CallContract<GValue<Map<?, ?>>>) contract);
            } else {
                throw new IllegalArgumentException("Unsupported StepContract type: " + contract.getClass().getName());
            }
        }

        for (Map.Entry<Step, P> entry : predicateRegistry.entrySet()) {
            extractPredicateGValues(entry.getValue());
        }

        locked = true;
    }

    /**
     * Determines if the manager is locked or not.
     */
    public boolean isLocked() {
        return locked;
    }

    public void merge(final GValueManager other) {
        if (this.locked) throw Traversal.Exceptions.traversalIsLocked();
        //TODO deal with conflicts
        gValueRegistry.putAll(other.gValueRegistry);
        stepRegistry.putAll(other.stepRegistry);
        predicateRegistry.putAll(other.predicateRegistry);
    }

    /**
     * Copy the registry state from one step to another.
     */
    public <S, E> void copyRegistryState(final Step<S,E> sourceStep, final Step<S,E> targetStep) {
        if (this.locked) throw Traversal.Exceptions.traversalIsLocked();
        if (stepRegistry.containsKey(sourceStep)) {
            stepRegistry.put(targetStep, stepRegistry.get(sourceStep));
        }

        if (predicateRegistry.containsKey(sourceStep)) {
            predicateRegistry.put(targetStep, predicateRegistry.get(sourceStep));
        }
    }

    /**
     * Retrieves the {@link StepContract} associated with the given {@link Step}.  This method uses the internal step
     * registry to fetch the associated contract. It is expected that the step has been previously registered with a
     * corresponding contract.
     */
    public <T extends StepContract> T getStepContract(final Step step) {
        return (T) stepRegistry.get(step);
    }

    /**
     * Determines whether the given step is parameterized by checking its presence
     * in the step registry or the predicate registry.
     *
     * @param step the {@link Step} to be checked
     * @return {@code true} if the step is present in the step registry or predicate registry,
     *         {@code false} otherwise
     */
    public <S> boolean isParameterized(final Step step) {
        return this.stepRegistry.containsKey(step) || this.predicateRegistry.containsKey(step);
    }

    /**
     * Gets the set of variable names used in this traversal. Note that this set won't be consistent until
     * {@link #lock()} is called first.
     */
    public Set<String> variableNames() {
        return Collections.unmodifiableSet(gValueRegistry.keySet());
    }


    /**
     * Determines whether the GValueManager contains any registered GValues, steps, or predicates.
     */
    public boolean isEmpty() {
        return gValueRegistry.isEmpty() && stepRegistry.isEmpty() && predicateRegistry.isEmpty();
    }

    /**
     * Delete all data
     */
    public void reset() {
        stepRegistry.clear();
        gValueRegistry.clear();
        predicateRegistry.clear();
    }

    /**
     * Removes the given {@link Step} from the registry.
     * If the manager is locked, the operation is not permitted and an exception is thrown.
     *
     * @param step the {@link Step} to be removed from the manager
     * @throws IllegalStateException if the manager is locked
     */
    public void remove(final Step step) {
        if (this.locked) throw Traversal.Exceptions.traversalIsLocked();
        stepRegistry.remove(step);
        predicateRegistry.remove(step);
    }

    private void extractPredicateGValues(final P predicate) {
        if (predicate.getValue() instanceof GValue) {
            GValue<?> gValue = (GValue<?>) predicate.getValue();
            if (gValue.isVariable()) {
                gValueRegistry.put(gValue.getName(), gValue);
            }
        }
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private void extractGValue(final RangeContract<GValue<Long>> contract) {
        if (contract.getHighRange().isVariable()) {
            gValueRegistry.put(contract.getHighRange().getName(), contract.getHighRange());
        }
        if (contract.getLowRange().isVariable()) {
            gValueRegistry.put(contract.getLowRange().getName(), contract.getLowRange());
        }
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private void extractGValue(final TailContract<GValue<Long>> contract) {
        if (contract.getLimit().getName() != null) {
            gValueRegistry.put(contract.getLimit().getName(), contract.getLimit());
        }
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private void extractGValue(final MergeElementContract<GValue<Map<?,?>>> contract) {
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
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private void extractGValue(final ElementIdsContract<GValue> contract) {
        for (GValue gValue: contract.getIds()) {
            if (gValue.isVariable()) {
                gValueRegistry.put(gValue.getName(), gValue);
            }
        }
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private void extractGValue(final AddVertexContract<GValue<String>, ?, GValue<?>> contract) {
        gValueRegistry.put(contract.getLabel().getName(), contract.getLabel());
        for (GValue<?> value : contract.getProperties().values()) {
            gValueRegistry.put(value.getName(), value);
        }
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private void extractGValue(final AddEdgeContract<GValue<String>, GValue<Vertex>, ?, GValue<?>> contract) {
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
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private void extractGValue(final AddPropertyContract<?, GValue<?>> contract) {
        gValueRegistry.put(contract.getValue().getName(), contract.getValue());
        for (GValue<?> value : contract.getProperties().values()) {
            gValueRegistry.put(value.getName(), value);
        }
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private void extractGValue(final EdgeLabelContract<GValue<String>> contract) {
        for (GValue gValue: contract.getEdgeLabels()) {
            if (gValue.getName() != null) {
                gValueRegistry.put(gValue.getName(), gValue);
            }
        }
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private void extractGValue(final CallContract<GValue<Map<?,?>>> contract) {
        if (contract.getStaticParams().getName() != null) {
            gValueRegistry.put(contract.getStaticParams().getName(), contract.getStaticParams());
        }
    }
}
