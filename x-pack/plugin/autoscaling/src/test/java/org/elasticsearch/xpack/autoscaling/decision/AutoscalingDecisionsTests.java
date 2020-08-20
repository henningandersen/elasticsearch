/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class AutoscalingDecisionsTests extends AutoscalingTestCase {

    public void testAutoscalingDecisionsRejectsEmptyDecisions() {
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new AutoscalingDecisions(randomAlphaOfLength(10),
            new AutoscalingCapacity(randomStorageAndMemory(), randomStorageAndMemory()), List.of()));
        assertThat(e.getMessage(), equalTo("decisions can not be empty"));
    }

    // todo: add summation tests.

    public void testRequiredCapacity() {
        AutoscalingCapacity single = randomBoolean() ? randomAutoscalingCapacity() : null;
        verifyRequiredCapacity(single, single);
        // any undecided decider nulls out any decision making
        verifyRequiredCapacity(null, single, null);
        verifyRequiredCapacity(null, null, single);

        boolean node = randomBoolean();
        boolean storage = randomBoolean();
        boolean memory = randomBoolean() || storage == false;

        AutoscalingCapacity large = randomCapacity(node, storage, memory, 1000, 2000);

        List<AutoscalingCapacity> autoscalingCapacities = new ArrayList<>();
        autoscalingCapacities.add(large);
        IntStream.range(0, 10).mapToObj(i -> randomCapacity(node, storage, memory, 0, 1000)).forEach(autoscalingCapacities::add);

        Randomness.shuffle(autoscalingCapacities);
        verifyRequiredCapacity(large, autoscalingCapacities.toArray(AutoscalingCapacity[]::new));

        for (Consumer<>)
        AutoscalingCapacity largerStorage = randomCapacity(node, true, false, 2000, 3000);
        autoscalingCapacities.add(largerStorage);
        Randomness.shuffle(autoscalingCapacities);
        AutoscalingCapacity.Builder expectedBuilder = AutoscalingCapacity.builder().tier(largerStorage.tier().storage(),
            large.tier().memory());
        if (node) {
            expectedBuilder.node(largerStorage.tier().storage(), large.tier().memory());
        }
        verifyRequiredCapacity(expectedBuilder.build(), autoscalingCapacities.toArray(AutoscalingCapacity[]::new));
    }

    private AutoscalingCapacity randomCapacity(boolean node, boolean storage, boolean memory, int lower, int upper) {
        AutoscalingCapacity.Builder builder = AutoscalingCapacity.builder();
        builder.tier(storage ? randomLongBetween(lower, upper) : null,
            memory ? randomLongBetween(lower, upper) : null);
        if (node) {
            builder.node(storage ? randomLongBetween(lower, upper) : null,
                memory ? randomLongBetween(lower, upper) : null);
        }
        return builder.build();
    }

    private void verifyRequiredCapacity(AutoscalingCapacity expected, AutoscalingCapacity... capacities) {
        List<AutoscalingDecision> decisions = Arrays.stream(capacities).map(AutoscalingDecisionsTests::randomAutoscalingDecisionWithCapacity).collect(Collectors.toList());
        assertThat(new AutoscalingDecisions(randomAlphaOfLength(10), randomAutoscalingCapacity(), decisions).requiredCapacity(),
            equalTo(expected));
    }
}
