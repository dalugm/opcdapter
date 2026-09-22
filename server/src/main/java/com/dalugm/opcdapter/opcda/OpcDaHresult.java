/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

import java.util.Map;
import java.util.Optional;

import com.dalugm.opcdapter.api.opcda.v1.ErrorCode;

public enum OpcDaHresult {
    UNKNOWN_ITEM_ID(0xC0040007, ErrorCode.ERROR_CODE_ITEM_NOT_FOUND, "OPC item not found");

    private final int value;
    private final ErrorCode errorCode;
    private final String message;
    private static final Map<Integer, OpcDaHresult> BY_VALUE =
            Map.of(UNKNOWN_ITEM_ID.value, UNKNOWN_ITEM_ID);

    OpcDaHresult(int value, ErrorCode errorCode, String message) {
        this.value = value;
        this.errorCode = errorCode;
        this.message = message;
    }

    public static Optional<OpcDaHresult> fromValue(int value) {
        return Optional.ofNullable(BY_VALUE.get(value));
    }

    public ErrorCode errorCode() {
        return errorCode;
    }

    public int value() {
        return value;
    }

    public String message() {
        return message;
    }
}
