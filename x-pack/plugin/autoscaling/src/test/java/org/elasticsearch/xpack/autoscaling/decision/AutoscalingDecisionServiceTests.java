/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class AutoscalingDecisionServiceTests extends AutoscalingTestCase {
    public void testMultiplePoliciesFixedDecision() {
        AutoscalingDecisionService service = new AutoscalingDecisionService(Set.of(new FixedAutoscalingDeciderService()));
        Set<String> policyNames = IntStream.range(0, randomIntBetween(1, 10))
            .mapToObj(i -> "test_ " + randomAlphaOfLength(10))
            .collect(Collectors.toSet());

        SortedMap<String, AutoscalingPolicyMetadata> policies = new TreeMap<>(
            policyNames.stream()
                .map(s -> Tuple.tuple(s, new AutoscalingPolicyMetadata(new AutoscalingPolicy(s, randomFixedDeciders()))))
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2))
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().putCustom(AutoscalingMetadata.NAME, new AutoscalingMetadata(policies)))
            .build();
        SortedMap<String, AutoscalingDecisions> decisions = service.decide(state, new ClusterInfo() {
        });
        assertThat(decisions.keySet(), equalTo(policyNames));
        for (Map.Entry<String, AutoscalingDecisions> entry : decisions.entrySet()) {
            AutoscalingDecisions decision = entry.getValue();
            assertThat(decision.tier(), equalTo(entry.getKey()));
            Collection<AutoscalingDeciderConfiguration> deciders = policies.get(decision.tier()).policy().deciders().values();
            assertThat(deciders.size(), equalTo(1));
            FixedAutoscalingDeciderConfiguration configuration = (FixedAutoscalingDeciderConfiguration) deciders.iterator().next();
            AutoscalingCapacity requiredCapacity = calculateFixedDecisionCapacity(configuration);
            assertThat(decision.requiredCapacity(), equalTo(requiredCapacity));
            assertThat(decision.decisions().size(), equalTo(1));
            AutoscalingDecision deciderDecision = decision.decisions().iterator().next();
            assertThat(deciderDecision.requiredCapacity(), equalTo(requiredCapacity));
            ByteSizeValue storage = configuration.storage();
            ByteSizeValue memory = configuration.memory();
            int nodes = configuration.nodes();
            assertThat(deciderDecision.reason(), equalTo(new FixedAutoscalingDeciderService.FixedReason(storage, memory, nodes)));
            assertThat(
                deciderDecision.reason().getSummary(),
                equalTo("fixed storage [" + storage + "] memory [" + memory + "] nodes [" + nodes + "]")
            );

            // there is no nodes in any tier.
            assertThat(decision.currentCapacity(), equalTo(AutoscalingCapacity.ZERO));
        }
    }

    private SortedMap<String, AutoscalingDeciderConfiguration> randomFixedDeciders() {
        return new TreeMap<>(
            Map.of(
                FixedAutoscalingDeciderConfiguration.NAME,
                new FixedAutoscalingDeciderConfiguration(
                    randomNullableByteSizeValue(),
                    randomNullableByteSizeValue(),
                    randomIntBetween(1, 10)
                )
            )
        );
    }

    private AutoscalingCapacity calculateFixedDecisionCapacity(FixedAutoscalingDeciderConfiguration configuration) {
        ByteSizeValue totalStorage = configuration.storage() != null
            ? new ByteSizeValue(configuration.storage().getBytes() * configuration.nodes())
            : null;
        ByteSizeValue totalMemory = configuration.memory() != null
            ? new ByteSizeValue(configuration.memory().getBytes() * configuration.nodes())
            : null;

        if (totalStorage == null && totalMemory == null) {
            return null;
        } else {
            return new AutoscalingCapacity(
                new AutoscalingCapacity.StorageAndMemory(totalStorage, totalMemory),
                new AutoscalingCapacity.StorageAndMemory(configuration.storage(), configuration.memory())
            );
        }
    }
}
