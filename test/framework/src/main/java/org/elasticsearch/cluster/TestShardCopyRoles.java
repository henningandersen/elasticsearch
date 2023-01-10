/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.routing.ShardCopyRoleFactory;

import java.util.List;

public class TestShardCopyRoles {
    public static final ShardCopyRoleFactory DEFAULT_ROLE_ONLY = ClusterModule.getShardCopyRoleFactory(List.of());

    private TestShardCopyRoles() {
        // no instances
    }
}
