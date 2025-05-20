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
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.EdgeLabelContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.RangeContract;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Arrays;
import java.util.HashMap;
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
 */
public class GValueManagerVerifier {

    /**
     * Creates a verification builder for the given traversal and strategy
     */
    public static <S, E> VerificationBuilder<S, E> verify(final Traversal.Admin<S, E> traversal, final TraversalStrategy strategy) {
        return new VerificationBuilder<>(traversal, strategy);
    }

    /**
     * Builder for configuring verification parameters
     */
    public static class VerificationBuilder<S, E> {
        private final Traversal.Admin<S, E> traversal;
        private final TraversalStrategy strategy;

        private VerificationBuilder(final Traversal.Admin<S, E> traversal, final TraversalStrategy strategy) {
            this.traversal = traversal;
            this.strategy = strategy;
        }

        public BeforeVerifier<S, E> beforeApplying() {
            // Capture pre-strategy state
            final Map<Step, Set<String>> preStepVariables = captureStepVariables(traversal);
            final Set<String> preVariables = new HashSet<>(traversal.getGValueManager().variableNames());

            return new BeforeVerifier<>(traversal, preVariables, preStepVariables);
        }

        /**
         * Applies the strategy and returns the verifier
         */
        public AfterVerifier<S, E> afterApplying() {
            // Capture pre-strategy state
            final Map<Step, Set<String>> preStepVariables = captureStepVariables(traversal);
            final Set<String> preVariables = new HashSet<>(traversal.getGValueManager().variableNames());

            // Apply strategy
            final TraversalStrategies strategies = new DefaultTraversalStrategies();
            strategies.addStrategies(strategy);
            traversal.setStrategies(strategies);
            traversal.applyStrategies();

            return new AfterVerifier<>(traversal, preVariables, preStepVariables);
        }

        private Map<Step, Set<String>> captureStepVariables(final Traversal.Admin<?, ?> traversal) {
            final Map<Step, Set<String>> result = new HashMap<>();
            final GValueManager manager = traversal.getGValueManager();

            for (Step step : traversal.getSteps()) {
                if (manager.isParameterized(step)) {
                    result.put(step, extractStepVariables(step, manager));
                }
            }
            return result;
        }

        private Set<String> extractStepVariables(final Step step, final GValueManager manager) {
            final Set<String> variables = new HashSet<>();
            final Object contract = manager.getStepContract(step);

            if (contract instanceof EdgeLabelContract) {
                final EdgeLabelContract<GValue<String>> edgeLabelContract = (EdgeLabelContract<GValue<String>>) contract;
                for (GValue<String> label : edgeLabelContract.getEdgeLabels()) {
                    if (label.isVariable()) variables.add(label.getName());
                }
            } else if (contract instanceof RangeContract) {
                final RangeContract<GValue<Long>> rangeContract = (RangeContract<GValue<Long>>) contract;
                if (rangeContract.getLowRange().isVariable())
                    variables.add(rangeContract.getLowRange().getName());
                if (rangeContract.getHighRange().isVariable())
                    variables.add(rangeContract.getHighRange().getName());
            }
            // Add more contract type handling as needed

            return variables;
        }
    }

    /**
     * Provides verification methods after strategy application
     */
    public static class AfterVerifier<S, E> extends AbstractVerifier<S, E, AfterVerifier<S, E>> {
        private AfterVerifier(final Traversal.Admin<S, E> traversal,
                              final Set<String> preVariables,
                              final Map<Step, Set<String>> preStepVariables) {
            super(traversal, preVariables, preStepVariables);
        }

        @Override
        protected AfterVerifier<S, E> self() {
            return this;
        }

        /**
         * Verifies that all variables are preserved
         */
        public AfterVerifier<S, E> variablesArePreserved() {
            final Set<String> currentVariables = traversal.getGValueManager().variableNames();
            assertEquals("All variables should be preserved", preVariables, currentVariables);
            return this;
        }

        /**
         * Verifies that all variables are removed
         */
        public AfterVerifier<S, E> variablesAreRemoved() {
            final Set<String> currentVariables = traversal.getGValueManager().variableNames();
            assertThat("All variables should be removed", currentVariables.isEmpty(), is(true));
            return this;
        }

        /**
         * Verifies that variables have been transferred between steps
         */
        public AfterVerifier<S, E> variablesHaveBeenTransferredFrom(final Step originalStep) {
            if (!preStepVariables.containsKey(originalStep)) {
                fail("Original step was not parameterized before strategy application");
            }

            final Set<String> originalVariables = preStepVariables.get(originalStep);
            final Set<String> currentVariables = traversal.getGValueManager().variableNames();

            assertThat("Variables should be transferred",
                    currentVariables.containsAll(originalVariables), is(true));

            return this;
        }
    }

    /**
     * Abstract base class for verification methods
     */
    public static abstract class AbstractVerifier<S, E, T extends AbstractVerifier<S, E, T>> {
        protected final Traversal.Admin<S, E> traversal;
        protected final Set<String> preVariables;
        protected final Map<Step, Set<String>> preStepVariables;

        protected AbstractVerifier(final Traversal.Admin<S, E> traversal,
                                   final Set<String> preVariables,
                                   final Map<Step, Set<String>> preStepVariables) {
            this.traversal = traversal;
            this.preVariables = preVariables;
            this.preStepVariables = preStepVariables;
        }

        /**
         * Returns this instance cast to the implementing class type
         */
        protected abstract T self();

        /**
         * Verifies that specific variables exist
         */
        public T hasVariables(final String... variables) {
            final Set<String> currentVariables = traversal.getGValueManager().variableNames();
            final Set<String> expected = new HashSet<>(Arrays.asList(variables));
            assertEquals("Variables should match expected set", expected, currentVariables);
            return self();
        }

        /**
         * Verifies that GValueManager is empty
         */
        public T managerIsEmpty() {
            assertThat("GValueManager should be empty", traversal.getGValueManager().isEmpty(), is(true));
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
            final GValueManager manager = traversal.getGValueManager();
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
            final GValueManager manager = traversal.getGValueManager();
            assertThat("Step should be parameterized", manager.isParameterized(step), is(true));

            final RangeContract<GValue<Long>> contract = manager.getStepContract(step);
            assertNotNull("RangeContract should exist", contract);

            assertEquals("Low range should match", expectedLow, contract.getLowRange().get().longValue());
            assertEquals("High range should match", expectedHigh, contract.getHighRange().get().longValue());
            assertEquals("Low range name should match", lowName, contract.getLowRange().getName());
            assertEquals("High range name should match", highName, contract.getHighRange().getName());

            return self();
        }
    }

    /**
     * Provides verification methods before strategy applications
     */
    public static class BeforeVerifier<S, E> extends AbstractVerifier<S, E, BeforeVerifier<S, E>> {
        private BeforeVerifier(final Traversal.Admin<S, E> traversal,
                               final Set<String> preVariables,
                               final Map<Step, Set<String>> preStepVariables) {
            super(traversal, preVariables, preStepVariables);
        }

        @Override
        protected BeforeVerifier<S, E> self() {
            return this;
        }
    }
}
