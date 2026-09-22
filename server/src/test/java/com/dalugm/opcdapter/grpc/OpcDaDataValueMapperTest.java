/* (C) 2026 */
package com.dalugm.opcdapter.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

import com.dalugm.opcdapter.api.opcda.v1.ErrorCode;
import com.dalugm.opcdapter.opcda.ReadResult;

class OpcDaDataValueMapperTest {

    @Test
    void preservesDeadlineExceededErrorCode() {
        var mapped =
                new OpcDaDataValueMapper()
                        .toDataValue(
                                "connection-1",
                                ReadResult.failure(
                                        "Point1",
                                        0,
                                        0,
                                        ErrorCode.ERROR_CODE_DEADLINE_EXCEEDED,
                                        "timed out"));

        assertEquals(ErrorCode.ERROR_CODE_DEADLINE_EXCEEDED, mapped.getError().getCode());
    }

    @Test
    void doesNotExposeJavaImplementationClassForUnsupportedValues() {
        var mapped =
                new OpcDaDataValueMapper()
                        .toDataValue(
                                "connection-1", ReadResult.success("Point1", new Object(), 0, 0));

        assertEquals(ErrorCode.ERROR_CODE_UNSUPPORTED_VALUE_TYPE, mapped.getError().getCode());
        assertEquals("OPC value type is not supported", mapped.getError().getMessage());
        assertFalse(mapped.getError().getMessage().contains("java.lang"));
    }
}
