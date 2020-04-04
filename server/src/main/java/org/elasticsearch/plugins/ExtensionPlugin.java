/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.common.inject.TypeLiteral;

import java.util.Collection;
import java.util.function.Function;

public interface ExtensionPlugin {
    interface Extension<P> {
        /**
         * Add a {@code Set<T>} type that is available for injection.
         */
        <T> void addLazySet(TypeLiteral<T> type, Function<P, Collection<Class<? extends T>>> pluginToConcreteTypes);
    }

    interface Extender {
        <P> Extension<P> extend(Class<P> pluginType);
    }

    <O> void extend(Extender extender);
}
