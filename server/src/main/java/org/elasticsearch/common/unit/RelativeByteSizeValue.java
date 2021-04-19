/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.ElasticsearchParseException;

/**
 * A byte size value that allows specification using either of:
 * 1. Absolute value (200GB for instance)
 * 2. Relative percentage value (95%, optionally with max headroom, 95%;max_headroom=100GB)
 * 3. Relative ratio value (0.95, , optionally with max headroom, 95%;max_headroom=100GB)
 */
public class RelativeByteSizeValue {

    public static final String MAX_HEADROOM_PREFIX = "max_headroom=";
    private final ByteSizeValue absolute;
    private final RatioValue ratio;
    private final ByteSizeValue maxHeadroom;

    public RelativeByteSizeValue(ByteSizeValue absolute) {
        this.absolute = absolute;
        this.ratio = null;
        this.maxHeadroom = ByteSizeValue.ZERO;
    }

    public RelativeByteSizeValue(RatioValue ratio, ByteSizeValue maxHeadroom) {
        this.absolute = null;
        this.ratio = ratio;
        this.maxHeadroom = maxHeadroom;
    }

    public ByteSizeValue calculateValue(ByteSizeValue total) {
        if (ratio != null) {
            long bytes = Math.max((long) (ratio.getAsRatio() * total.getBytes()), total.getBytes() - maxHeadroom.getBytes());
            return ByteSizeValue.ofBytes(bytes);
        } else {
            return absolute;
        }
    }

    public static RelativeByteSizeValue parseRelativeByteSizeValue(String value, String settingName) {
        int semicolonIndex = value.indexOf(';');
        if (semicolonIndex == -1) {
            try {
                return new RelativeByteSizeValue(ByteSizeValue.parseBytesSizeValue(value, settingName));
            } catch (ElasticsearchParseException e1) {
                // ignore, see if it parses as percent/ratio.
            }
        }

        String part1 = (semicolonIndex == -1 ? value : value.substring(0, semicolonIndex)).trim();
        RatioValue ratioValue;
        try {
            ratioValue = RatioValue.parseRatioValue(part1);
        } catch (ElasticsearchParseException e2) {
                throw new ElasticsearchParseException("unable to parse [%s=%s] as either percentage or bytes",
                    settingName, value);
        }

        return new RelativeByteSizeValue(ratioValue,
            semicolonIndex == -1 ? ByteSizeValue.ZERO : parseMaxHeadroom(value.substring(semicolonIndex + 1).trim(), value, settingName));
    }

    private static ByteSizeValue parseMaxHeadroom(String value, String originalValue, String settingName) {
        if (value.startsWith(MAX_HEADROOM_PREFIX)) {
            try {
                return ByteSizeValue.parseBytesSizeValue(value, settingName);
            } catch (ElasticsearchParseException e) {
                throw new ElasticsearchParseException("unable to parse max_headroom from [%s=%s]", settingName, originalValue);
            }
        }

        return ByteSizeValue.ZERO;
    }
}
