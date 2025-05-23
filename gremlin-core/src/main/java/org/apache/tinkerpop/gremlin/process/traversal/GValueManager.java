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

import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddEdgeContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddVertexContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.CallContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.ElementIdsContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.EdgeLabelContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddPropertyContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.MergeElementContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.PredicateContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.RangeContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.StepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.TailContract;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
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
public final class GValueManager implements Serializable, Cloneable {

    private boolean locked = false;
    private final Map<String, GValue> gValueRegistry = new IdentityHashMap();
    private final Map<Step, StepContract> stepRegistry = new IdentityHashMap();

    public GValueManager() {
        this(Collections.EMPTY_MAP, Collections.EMPTY_MAP);
    }

    private GValueManager(Map<String, GValue> gValueRegistry, Map<Step, StepContract> stepRegistry) {
        this.gValueRegistry.putAll(gValueRegistry);
        this.stepRegistry.putAll(stepRegistry);
    }

    /**
     * Register a step with any {@link StepContract}.
     */
    public void register(final Step step, final StepContract contract) {
        if (this.locked) throw Traversal.Exceptions.traversalIsLocked();
        stepRegistry.put(step, contract);
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

        for (StepContract contract : stepRegistry.values()) {
            registerGValues(extractGValues(contract));
        }

        locked = true;
    }

    private void registerGValues(Collection<GValue<?>> gValues) {
        for (GValue<?> gValue : gValues) {
            if (gValue.getName() != null) {
                gValueRegistry.put(gValue.getName(), gValue);
            }
        }
    }

    private void removeGValues(Collection<GValue<?>> gValues) {
        for (GValue<?> gValue : gValues) {
            gValueRegistry.remove(gValue.getName(), gValue);
            // TODO:: cascade to other steps
        }
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
    }

    /**
     * Copy the registry state from one step to another.
     */
    public <S, E> void copyRegistryState(final Step<S,E> sourceStep, final Step<S,E> targetStep) {
        if (this.locked) {
            throw Traversal.Exceptions.traversalIsLocked();
        }
        if (stepRegistry.containsKey(sourceStep)) {
            stepRegistry.put(targetStep, stepRegistry.get(sourceStep));
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
        return this.stepRegistry.containsKey(step);
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
        return gValueRegistry.isEmpty() && stepRegistry.isEmpty();
    }

    /**
     * Delete all data
     */
    public void reset() {
        stepRegistry.clear();
        gValueRegistry.clear();
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
        StepContract removedContract = stepRegistry.remove(step);

        if (removedContract != null) {
            removeGValues(extractGValues(removedContract));
        }
    }

    private Collection<GValue<?>> extractPredicateGValues(final P predicate) {
        return predicate.getGValueRegistry().getGValues();
    }

    private Collection<GValue<?>> extractGValues(final StepContract contract) {
        if (contract instanceof RangeContract) {
            return extractGValue((RangeContract<GValue<Long>>) contract);
        } else if (contract instanceof TailContract) {
            return extractGValue((TailContract<GValue<Long>>) contract);
        } else if (contract instanceof MergeElementContract) {
            return extractGValue((MergeElementContract<GValue<Map<?, ?>>> ) contract);
        } else if (contract instanceof ElementIdsContract) {
            return extractGValue((ElementIdsContract<GValue<?>>) contract);
        } else if (contract instanceof AddVertexContract) {
            return extractGValue((AddVertexContract<GValue<String>, ?, GValue<?>>) contract);
        } else if (contract instanceof AddEdgeContract) {
            return extractGValue((AddEdgeContract<GValue<String>, GValue<Vertex>, ?, GValue<?>>) contract);
        } else if (contract instanceof AddPropertyContract) {
            return extractGValue((AddPropertyContract<?, GValue<?>>) contract);
        } else if (contract instanceof EdgeLabelContract) {
            return extractGValue((EdgeLabelContract<GValue<String>>) contract);
        } else if (contract instanceof CallContract) {
            return extractGValue((CallContract<GValue<Map<?, ?>>>) contract);
        } else if (contract instanceof PredicateContract) {
            return extractGValue((PredicateContract) contract);
        } else if (contract instanceof HasContainerHolder) {
            return extractGValue((HasContainerHolder) contract);
        } else {
            throw new IllegalArgumentException("Unsupported StepContract type: " + contract.getClass().getName());
        }
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final RangeContract<GValue<Long>> contract) {
        Collection<GValue<?>> results = new ArrayList();
        if (contract.getHighRange().isVariable()) {
            results.add(contract.getHighRange());
        }
        if (contract.getLowRange().isVariable()) {
            results.add(contract.getLowRange());
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final TailContract<GValue<Long>> contract) {
        if (contract.getLimit().getName() != null) {
            return Collections.singletonList(contract.getLimit());
        }
        return Collections.emptyList();
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final MergeElementContract<GValue<Map<?,?>>> contract) {
        Collection<GValue<?>> results = new ArrayList();
        if (contract.getMergeMap() != null) {
            results.add(contract.getMergeMap());
        }
        if (contract.getOnCreateMap() != null) {
            results.add(contract.getOnCreateMap());
        }
        if (contract.getOnMatchMap() != null) {
            results.add(contract.getOnMatchMap());
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final ElementIdsContract<GValue<?>> contract) {
        Collection<GValue<?>> results = new ArrayList();
        for (GValue gValue: contract.getIds()) {
            if (gValue.isVariable()) {
                results.add(gValue);
            }
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final AddVertexContract<GValue<String>, ?, GValue<?>> contract) {
        Collection<GValue<?>> results = new ArrayList();
        if (contract.getLabel() != null) {
            results.add(contract.getLabel());
        }
        for (GValue<?> value : contract.getProperties().values()) {
            results.add(value);
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final AddEdgeContract<GValue<String>, GValue<Vertex>, ?, GValue<?>> contract) {
        Collection<GValue<?>> results = new ArrayList();
        if (contract.getLabel() != null) {
            results.add(contract.getLabel());
        }
        if (contract.getFrom() != null) {
            results.add(contract.getFrom());
        }
        if (contract.getTo() != null) {
            results.add(contract.getTo());
        }
        if (contract.getProperties() != null) {
            for (GValue<?> value : contract.getProperties().values()) {
                results.add(value);
            }
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final AddPropertyContract<?, GValue<?>> contract) {
        Collection<GValue<?>> results = new ArrayList();
        results.add(contract.getValue());
        for (GValue<?> value : contract.getProperties().values()) {
            results.add(value);
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final EdgeLabelContract<GValue<String>> contract) {
        Collection<GValue<?>> results = new ArrayList();
        for (GValue gValue: contract.getEdgeLabels()) {
            if (gValue.getName() != null) {
                results.add(gValue);
            }
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final CallContract<GValue<Map<?,?>>> contract) {
        Collection<GValue<?>> results = new ArrayList();
        if (contract.getStaticParams().getName() != null) {
            results.add(contract.getStaticParams());
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final PredicateContract contract) {
        if (contract.getPredicate() != null) {
            return contract.getPredicate().getGValueRegistry().getGValues();
        }
        return Collections.emptyList();
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final HasContainerHolder contract) {
        Collection<GValue<?>> results = new ArrayList();
        for (P<?> predicate : contract.getPredicates()) {
            if (predicate != null) {
                results.addAll(predicate.getGValueRegistry().getGValues());
            }
        }
        return results;
    }

    @Override
    public GValueManager clone() {
        return new GValueManager(gValueRegistry, stepRegistry);
    }
}
