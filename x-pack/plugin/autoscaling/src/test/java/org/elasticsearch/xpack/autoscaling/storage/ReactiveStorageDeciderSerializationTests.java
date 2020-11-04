/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.autoscaling.Autoscaling;

import java.io.IOException;

public class ReactiveStorageDeciderSerializationTests extends AbstractSerializingTestCase<ReactiveStorageDeciderService> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new Autoscaling(Settings.EMPTY).getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new Autoscaling(Settings.EMPTY).getNamedXContent());
    }

    @Override
    protected ReactiveStorageDeciderService doParseInstance(XContentParser parser) throws IOException {
        return ReactiveStorageDeciderService.parse(parser);
    }

    @Override
    protected Writeable.Reader<ReactiveStorageDeciderService> instanceReader() {
        return ReactiveStorageDeciderService::new;
    }

    @Override
    protected ReactiveStorageDeciderService createTestInstance() {
        return new ReactiveStorageDeciderService(randomAlphaOfLength(8), randomAlphaOfLength(8));
    }

    @Override
    protected ReactiveStorageDeciderService mutateInstance(ReactiveStorageDeciderService instance) throws IOException {
        boolean mutateAttribute = randomBoolean();
        return new ReactiveStorageDeciderService(
            mutateAttribute ? randomValueOtherThan(instance.getTierAttribute(), () -> randomAlphaOfLength(8)) : instance.getTierAttribute(),
            mutateAttribute == false || randomBoolean()
                ? randomValueOtherThan(instance.getTier(), () -> randomAlphaOfLength(8))
                : instance.getTier()
        );
    }
}
