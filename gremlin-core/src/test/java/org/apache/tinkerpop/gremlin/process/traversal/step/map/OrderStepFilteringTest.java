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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class OrderStepFilteringTest {

    @Test
    public void shouldFilterByDefaultAndOnlyEnableFiltering() throws Exception {
        final OrderGlobalStep<?, ?> global = globalOrder();
        final OrderLocalStep<?, ?> local = localOrder();

        assertTrue(global.isFilteringUnproductiveTraversers());
        assertTrue(local.isFilteringUnproductiveTraversers());
        global.setFilterUnproductiveTraversers(false);
        local.setFilterUnproductiveTraversers(false);
        assertTrue(global.isFilteringUnproductiveTraversers());
        assertTrue(local.isFilteringUnproductiveTraversers());
        disableFiltering(global);
        disableFiltering(local);
        global.setFilterUnproductiveTraversers(true);
        local.setFilterUnproductiveTraversers(true);
        assertTrue(global.isFilteringUnproductiveTraversers());
        assertTrue(local.isFilteringUnproductiveTraversers());
    }

    @Test
    public void shouldIncludeFilteringStateInHashCode() throws Exception {
        final OrderGlobalStep<?, ?> global = globalOrder();
        final OrderGlobalStep<?, ?> globalWithoutFiltering = globalOrder();
        final OrderLocalStep<?, ?> local = localOrder();
        final OrderLocalStep<?, ?> localWithoutFiltering = localOrder();
        disableFiltering(globalWithoutFiltering);
        disableFiltering(localWithoutFiltering);

        assertNotEquals(global.hashCode(), globalWithoutFiltering.hashCode());
        assertNotEquals(local.hashCode(), localWithoutFiltering.hashCode());
    }

    @Test
    public void shouldPreserveFilteringStateWhenCloned() throws Exception {
        final OrderGlobalStep<?, ?> global = globalOrder();
        final OrderLocalStep<?, ?> local = localOrder();
        disableFiltering(global);
        disableFiltering(local);

        final OrderGlobalStep<?, ?> globalClone = global.clone();
        final OrderLocalStep<?, ?> localClone = local.clone();

        assertTrue(!globalClone.isFilteringUnproductiveTraversers());
        assertTrue(!localClone.isFilteringUnproductiveTraversers());
    }

    private static OrderGlobalStep<?, ?> globalOrder() {
        return TraversalHelper.getFirstStepOfAssignableClass(OrderGlobalStep.class, __.order().asAdmin()).get();
    }

    private static OrderLocalStep<?, ?> localOrder() {
        return TraversalHelper.getFirstStepOfAssignableClass(
                OrderLocalStep.class, __.order(Scope.local).asAdmin()).get();
    }

    private static void disableFiltering(final Object orderStep) throws Exception {
        final Field field = orderStep.getClass().getDeclaredField("filterUnproductiveTraversers");
        field.setAccessible(true);
        field.setBoolean(orderStep, false);
    }
}
