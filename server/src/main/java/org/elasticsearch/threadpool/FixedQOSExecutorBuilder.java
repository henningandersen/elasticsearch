/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * A builder for fixed executors.
 */
public final class FixedQOSExecutorBuilder extends ExecutorBuilder<FixedQOSExecutorBuilder.FixedExecutorSettings> {

    private final Setting<Integer> sizeSetting;
    private final Setting<Integer> maxSizeSetting;
    private final Setting<Integer> queueSizeSetting;

    /**
     * Construct a fixed executor builder; the settings will have the key prefix "thread_pool." followed by the executor name.
     *
     * @param settings  the node-level settings
     * @param name      the name of the executor
     * @param size      the fixed number of threads
     * @param queueSize the size of the backing queue, -1 for unbounded
//     * @param trackEWMA whether to track the exponentially weighted moving average of the task execution time
     */
    FixedQOSExecutorBuilder(final Settings settings, final String name, final int size, final int maxSize, final int queueSize) {
        this(settings, name, size, maxSize, queueSize, "thread_pool." + name);
    }


    /**
     * Construct a fixed executor builder.
     *
     * @param settings  the node-level settings
     * @param name      the name of the executor
     * @param size      the fixed number of threads
     * @param maxSize   the maximum number of threads when some are paused
     * @param queueSize the size of the backing queue, -1 for unbounded
     * @param prefix    the prefix for the settings keys
     */
    public FixedQOSExecutorBuilder(
        final Settings settings,
        final String name,
        final int size,
        final int maxSize,
        final int queueSize,
        final String prefix
    ) {
        super(name);
        final String sizeKey = settingsKey(prefix, "size");
        this.sizeSetting = new Setting<>(
            sizeKey,
            s -> Integer.toString(size),
            s -> Setting.parseInt(s, 1, applyHardSizeLimit(settings, name), sizeKey),
            Setting.Property.NodeScope
        );
        final String maxSizeKey = settingsKey(prefix, "max_size");
        // todo later: add validation against size setting.
        this.maxSizeSetting = new Setting<>(
            maxSizeKey,
            s -> Integer.toString(maxSize),
            s -> Setting.parseInt(s, 1, Integer.MAX_VALUE, sizeKey),
            Setting.Property.NodeScope
        );
        final String queueSizeKey = settingsKey(prefix, "queue_size");
        this.queueSizeSetting = Setting.intSetting(queueSizeKey, queueSize, Setting.Property.NodeScope);
    }

    @Override
    public List<Setting<?>> getRegisteredSettings() {
        return Arrays.asList(sizeSetting, maxSizeSetting, queueSizeSetting);
    }

    @Override
    FixedExecutorSettings getSettings(Settings settings) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        final int size = sizeSetting.get(settings);
        final int maxSize = maxSizeSetting.get(settings);
        final int queueSize = queueSizeSetting.get(settings);
        return new FixedExecutorSettings(nodeName, size, maxSize, queueSize);
    }

    @Override
    ThreadPool.ExecutorHolder build(final FixedExecutorSettings settings, final ThreadContext threadContext) {
        int size = settings.size;
        int maxSize = settings.maxSize;
        int queueSize = settings.queueSize;
        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(EsExecutors.threadName(settings.nodeName, name()));
        final ExecutorService executor = EsExecutors.newFixedQOS(
            settings.nodeName + "/" + name(),
            size,
            maxSize,
            queueSize,
            threadFactory,
            threadContext
        );
        final ThreadPool.Info info = new ThreadPool.Info(
            name(),
            ThreadPool.ThreadPoolType.FIXED_QOS,
            size,
            maxSize,
            null,
            queueSize < 0 ? null : new SizeValue(queueSize)
        );
        return new ThreadPool.ExecutorHolder(executor, info);
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(
            Locale.ROOT,
            "name [%s], size [%d], max size [%d], queue size [%s]",
            info.getName(),
            info.getMin(),
            info.getMax(),
            info.getQueueSize() == null ? "unbounded" : info.getQueueSize()
        );
    }

    static class FixedExecutorSettings extends ExecutorSettings {

        private final int size;
        private final int maxSize;
        private final int queueSize;

        FixedExecutorSettings(final String nodeName, final int size, final int maxSize, final int queueSize) {
            super(nodeName);
            this.size = size;
            this.maxSize = maxSize;
            this.queueSize = queueSize;
        }

    }

}
