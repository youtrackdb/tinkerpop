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
package org.apache.tinkerpop.gremlin.process.traversal.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.GValueManager;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.EdgeLabelContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.RangeContract;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Provides utilities to verify the state and behavior of {@code GValueManager} during and after traversal strategy
 * application. It offers a builder pattern to configure and perform verification checks for traversals.
 * Multiple strategies can be applied in the order they are provided.
 */
public class GValueManagerVerifier {

    /**
     * Creates a verification builder for the given traversal and strategies
     */
    public static <S, E> VerificationBuilder<S, E> verify(final Traversal.Admin<S, E> traversal, final TraversalStrategy strategy) {
        return verify(traversal, strategy, Collections.emptySet());
    }

    /**
     * Creates a verification builder for the given traversal and strategies
     */
    public static <S, E> VerificationBuilder<S, E> verify(final Traversal.Admin<S, E> traversal, final TraversalStrategy strategy,
                                                          final Collection<TraversalStrategy> additionalStrategies) {
        // Create an array with FilterRankingStrategy as the first strategy
        TraversalStrategy[] strategies;

        if (additionalStrategies.isEmpty()) {
            // If no additional strategies, just use the one provided
            strategies = new TraversalStrategy[] { strategy };
        } else {
            // If there are additional strategies, combine them with one provided
            strategies = new TraversalStrategy[additionalStrategies.size() + 1];
            strategies[0] = FilterRankingStrategy.instance();

            int i = 1;
            for (TraversalStrategy ts : additionalStrategies) {
                strategies[i++] = ts;
            }
        }

        return new VerificationBuilder<>(traversal, strategies);
    }

    /**
     * Builder for configuring verification parameters
     */
    public static class VerificationBuilder<S, E> {
        private final Traversal.Admin<S, E> traversal;
        private final TraversalStrategy[] strategies;

        private VerificationBuilder(final Traversal.Admin<S, E> traversal, final TraversalStrategy... strategies) {
            this.traversal = traversal;
            this.strategies = strategies;
        }

        public BeforeVerifier<S, E> beforeApplying() {
            return new BeforeVerifier<>(traversal, this);
        }

        /**
         * Applies the strategies and returns the verifier
         */
        public AfterVerifier<S, E> afterApplying() {
            // Capture pre-strategy state
            final GValueManager manager = traversal.getGValueManager();
            final Map<Step, Set<GValue>> preStepVariables = manager.gatherStepGValues(traversal);
            final Set<String> preVariables = manager.variableNames();
            final Set<GValue> preGValues = manager.gValues();

            // Apply strategies
            final TraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
            for (TraversalStrategy strategy : strategies) {
                traversalStrategies.addStrategies(strategy);
            }
            traversal.setStrategies(traversalStrategies);
            traversal.applyStrategies();

            return new AfterVerifier<>(traversal, preVariables, preStepVariables, preGValues);
        }
    }

    /**
     * Provides verification methods before strategy applications
     */
    public static class BeforeVerifier<S, E> extends AbstractVerifier<S, E, BeforeVerifier<S, E>> {
        private final VerificationBuilder<S, E> verificationBuilder;

        private BeforeVerifier(final Traversal.Admin<S, E> traversal, final VerificationBuilder<S, E> verificationBuilder) {
            super(traversal);
            this.verificationBuilder = verificationBuilder;
        }

        @Override
        protected BeforeVerifier<S, E> self() {
            return this;
        }

        /**
         * Applies the strategy and returns the verifier
         */
        public AfterVerifier<S, E> afterApplying() {
            return verificationBuilder.afterApplying();
        }
    }

    /**
     * Provides verification methods after strategy application
     */
    public static class AfterVerifier<S, E> extends AbstractVerifier<S, E, AfterVerifier<S, E>> {
        protected final Set<String> preVariables;
        protected final Set<GValue> preGValues;
        protected final Map<Step, Set<GValue>> preStepGValues;
        protected final Map<Step, Set<String>> preStepVariables;

        private AfterVerifier(final Traversal.Admin<S, E> traversal,
                              final Set<String> preVariables,
                              final Map<Step, Set<GValue>> preStepGValues,
                              final Set<GValue> preGValues) {
            super(traversal);
            this.preVariables = preVariables;
            this.preStepGValues = preStepGValues;
            this.preGValues = preGValues;

            // compute the preStepVariables from the preStepGValues by converting the GValue to their name if
            // their isVariable is true
            this.preStepVariables = preStepGValues.entrySet().stream()
                    .collect(java.util.stream.Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().stream()
                                    .filter(GValue::isVariable)
                                    .map(GValue::getName)
                                    .collect(java.util.stream.Collectors.toSet())
                    ));
        }

        @Override
        protected AfterVerifier<S, E> self() {
            return this;
        }

        /**
         * Verifies that all variables are preserved
         */
        public AfterVerifier<S, E> variablesArePreserved() {
            final Set<String> currentVariables = manager.variableNames();
            assertEquals("All variables should be preserved", preVariables, currentVariables);
            return this;
        }
    }

    /**
     * Abstract base class for verification methods
     */
    public static abstract class AbstractVerifier<S, E, T extends AbstractVerifier<S, E, T>> {
        protected final Traversal.Admin<S, E> traversal;
        protected final GValueManager manager;

        protected AbstractVerifier(final Traversal.Admin<S, E> traversal) {
            this.traversal = traversal;
            this.manager = traversal.getGValueManager();
        }

        /**
         * Returns this instance cast to the implementing class type
         */
        protected abstract T self();

        /**
         * Verifies that GValueManager is empty
         */
        public T managerIsEmpty() {
            assertThat("GValueManager should be empty", manager.isEmpty(), is(true));
            return self();
        }

        /**
         * Verifies that specific steps have been cleared from GValueManager
         */
        public T stepsAreParameterized(final boolean isParameterized, final Step... steps) {
            assertThat("At least one step must be provided", steps.length > 0, is(true));
            for (Step step : steps) {
                final GValueManager manager = step.getTraversal().getGValueManager();
                assertThat("Step should not be parameterized", manager.isParameterized(step), is(isParameterized));
            }
            return self();
        }

        /**
         * Verifies that all steps of a certain class are not parameterized
         */
        public <U extends Step> T stepsOfClassAreParameterized(final boolean isParameterized, final Class<U> stepClass) {
            final List<U> steps = TraversalHelper.getStepsOfAssignableClassRecursively(stepClass, traversal);
            return stepsAreParameterized(isParameterized, steps.toArray(new Step[steps.size()]));
        }

        /**
         * Verifies that an EdgeLabelContract is properly set up
         */
        public T edgeLabelContractIsValid(final Step step, final int expectedCount,
                                          final String[] expectedNames,
                                          final String[] expectedValues) {
            assertThat("Step should be parameterized", manager.isParameterized(step), is(true));

            final EdgeLabelContract<GValue<String>> contract = manager.getStepContract(step);
            assertNotNull("EdgeLabelContract should exist", contract);

            final GValue<String>[] edgeLabels = contract.getEdgeLabels();
            assertEquals("EdgeLabelContract should have correct label count",
                    expectedCount, edgeLabels.length);

            for (int i = 0; i < expectedCount; i++) {
                assertEquals("Label name should match", expectedNames[i], edgeLabels[i].getName());
                assertEquals("Label value should match", expectedValues[i], edgeLabels[i].get());
            }

            return self();
        }

        /**
         * Verifies that a RangeContract is properly set up
         */
        public T rangeContractIsValid(final Step step, final long expectedLow, final long expectedHigh,
                                      final String lowName, final String highName) {
            assertThat("Step should be parameterized", manager.isParameterized(step), is(true));

            final RangeContract<GValue<Long>> contract = manager.getStepContract(step);
            assertNotNull("RangeContract should exist", contract);

            assertEquals("Low range should match", expectedLow, contract.getLowRange().get().longValue());
            assertEquals("High range should match", expectedHigh, contract.getHighRange().get().longValue());
            assertEquals("Low range name should match", lowName, contract.getLowRange().getName());
            assertEquals("High range name should match", highName, contract.getHighRange().getName());

            return self();
        }

        /**
         * Verifies that specific variables exist.
         */
        public T hasVariables(final String... variables) {
            return hasVariables(CollectionUtil.asSet(variables));
        }

        /**
         * Verifies that specific variables exist.
         */
        public T hasVariables(final Set<String> variables) {
            // Get variables from the current traversal and all child traversals
            final Set<String> currentVariables = manager.variableNames();
            assertEquals("Variables should match expected set", variables, currentVariables);
            return self();
        }
    }
}
