/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.action.bulk.TxID;
import org.elasticsearch.index.translog.Translog;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ShardTransactionRegistry {
    private final Map<String, Set<TxID>> byKey = new HashMap<>();
    private final Map<TxID, Set<String>> byTxID = new HashMap<>();
    private final Map<TxID, Set<String>> conflictingKeysByTxID = new HashMap<>();
    private final Map<TxID, Translog.Location> translogHeads = new HashMap<>();

    private final Set<TxID> prepared = new HashSet<>();
    // todo: less locking and perhaps totally different content...
    public synchronized void registerTransaction(TxID txID, Set<String> ids) {
        Set<String> previous = byTxID.put(txID, ids);
        assert previous == null;
        for (String id : ids) {
            Set<TxID> txIDSet = byKey.computeIfAbsent(id, k -> new HashSet<>());
            txIDSet.add(txID);
            if (txIDSet.size() > 1) {
                txIDSet.forEach(conflict -> conflictingKeysByTxID.computeIfAbsent(conflict, k-> new HashSet<>()).add(id));
            }
        }

        assert invariant();
    }


    public synchronized void releaseTransaction(TxID txID) {
        byTxID.remove(txID).forEach(id -> cleanByKey(id, txID));
        prepared.remove(txID);
        conflictingKeysByTxID.remove(txID);
        assert invariant();
    }

    public synchronized void loggingComplete(TxID txID, Translog.Location headOfTranslogList) {
        assert translogHeads.containsKey(txID) == false;

        translogHeads.put(txID, headOfTranslogList);
    }

    public synchronized Map<TxID, Boolean> prepare(TxID txID) {
        assert byTxID.containsKey(txID);
        assert prepared.contains(txID) == false;
        prepared.add(txID);
        Set<String> conflictingKeys = conflictingKeysByTxID.get(txID);
        if (conflictingKeys != null) {
            return conflictingKeys.stream().flatMap(id -> byKey.get(id).stream())
                .filter(conflict -> conflict.equals(txID) == false).distinct().collect(Collectors.toMap(Function.identity(),
                this::winConflict));
        } else {
            return Map.of();
        }
    }

    private boolean winConflict(TxID conflict) {
        return prepared.contains(conflict) == false;
    }

    private void cleanByKey(String id, TxID txID) {
        assert Thread.holdsLock(this);
        Set<TxID> txIDs = byKey.get(id);
        txIDs.remove(txID);
        if (txIDs.isEmpty()) {
            byKey.remove(id);
        } else if (txIDs.size() == 1) {
            conflictingKeysByTxID.get(txIDs.iterator().next()).remove(id);
        }
    }

    private boolean invariant() {
        byTxID.forEach((txID, keys) -> {
            keys.forEach(key -> {
                assert byKey.containsKey(key);
                assert byKey.get(key).contains(txID);
            });
        });
        byKey.forEach((key, txs) -> {
            txs.forEach(txID -> {
                assert byTxID.containsKey(txID);
                assert byTxID.get(txID).contains(key);
            });
        });
        prepared.forEach(txID -> {
            assert byTxID.containsKey(txID);
        });
        return true;
    }

    Set<String> keys(TxID txID) {
        return byTxID.get(txID);
    }

    public synchronized int size() {
        return byTxID.size();
    }

    public synchronized Translog.Location translogHead(TxID txID) {
        return translogHeads.get(txID);
    }
}
