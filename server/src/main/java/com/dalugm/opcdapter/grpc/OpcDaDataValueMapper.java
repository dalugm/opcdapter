/* (C) 2026 */
package com.dalugm.opcdapter.grpc;

import org.jinterop.dcom.core.IJIUnsigned;
import org.jinterop.dcom.core.JIArray;
import org.jinterop.dcom.core.JIVariant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.dalugm.opcdapter.api.opcda.v1.DataValue;
import com.dalugm.opcdapter.api.opcda.v1.ErrorCode;
import com.dalugm.opcdapter.api.opcda.v1.ErrorInfo;
import com.dalugm.opcdapter.opcda.ReadResult;

/** Maps Utgard/J-Interop values to the protocol's tagged DataValue representation. */
final class OpcDaDataValueMapper {

    private static final Logger log = LoggerFactory.getLogger(OpcDaDataValueMapper.class);

    DataValue toDataValue(String connectionId, ReadResult result) {
        DataValue.Builder builder =
                DataValue.newBuilder()
                        .setItemId(result.itemId())
                        .setQuality(result.quality())
                        .setSourceTimestampMs(result.sourceTimestampMs());
        return switch (result.outcome()) {
            case ReadResult.Failure failure -> builder.setError(errorInfo(failure)).build();
            case ReadResult.Value success ->
                    mapValue(connectionId, result.itemId(), builder, success.value());
        };
    }

    private DataValue mapValue(
            String connectionId, String itemId, DataValue.Builder builder, Object value) {
        boolean supported = setDataValue(builder, value);
        if (!supported) {
            builder.setError(
                    errorInfo(
                            ErrorCode.ERROR_CODE_UNSUPPORTED_VALUE_TYPE,
                            "OPC value type is not supported"));
            log.debug(
                    "unsupported OPC value type for {}:{}: {}",
                    connectionId,
                    itemId,
                    value.getClass().getName());
        }
        return builder.build();
    }

    private boolean setDataValue(DataValue.Builder builder, Object value) {
        return switch (value) {
            case null -> false;
            case Boolean bool -> {
                builder.setBoolValue(bool);
                yield true;
            }
            case Byte number -> {
                builder.setInt16Value(number);
                yield true;
            }
            case Short number -> {
                builder.setInt16Value(number);
                yield true;
            }
            case Integer number -> {
                builder.setInt32Value(number);
                yield true;
            }
            case Long number -> {
                builder.setInt64Value(number);
                yield true;
            }
            case Float number -> {
                builder.setFloatValue(number);
                yield true;
            }
            case Double number -> {
                builder.setDoubleValue(number);
                yield true;
            }
            case String text -> {
                builder.setStringValue(text);
                yield true;
            }
            case Character character -> {
                builder.setStringValue(character.toString());
                yield true;
            }
            case IJIUnsigned unsigned -> setUnsignedValue(builder, unsigned);
            case byte[] bytes -> {
                builder.setBytesValue(ByteString.copyFrom(bytes));
                yield true;
            }
            case JIArray array -> setArrayValue(builder, array);
            case java.util.Calendar calendar -> {
                builder.setDatetimeValue(calendar.getTimeInMillis());
                yield true;
            }
            case java.util.Date date -> {
                builder.setDatetimeValue(date.getTime());
                yield true;
            }
            default -> false;
        };
    }

    private boolean setUnsignedValue(DataValue.Builder builder, IJIUnsigned unsigned) {
        Number value = unsigned.getValue();
        return switch (unsigned.getType()) {
            case JIVariant.VT_UI1, JIVariant.VT_UI2 -> {
                builder.setUint16Value(value.intValue());
                yield true;
            }
            case JIVariant.VT_UI4 -> {
                builder.setUint32Value(value.intValue());
                yield true;
            }
            default -> false;
        };
    }

    private boolean setArrayValue(DataValue.Builder builder, JIArray array) {
        return switch (array.getArrayInstance()) {
            case null -> false;
            case byte[] bytes -> {
                builder.setBytesValue(ByteString.copyFrom(bytes));
                yield true;
            }
            case Byte[] bytes -> {
                byte[] primitive = new byte[bytes.length];
                for (int index = 0; index < bytes.length; index++) {
                    primitive[index] = bytes[index];
                }
                builder.setBytesValue(ByteString.copyFrom(primitive));
                yield true;
            }
            default -> false;
        };
    }

    private ErrorInfo errorInfo(ErrorCode code, String message) {
        return ErrorInfo.newBuilder()
                .setCode(code)
                .setMessage(message == null ? "unknown error" : message)
                .build();
    }

    private ErrorInfo errorInfo(ReadResult.Failure failure) {
        ErrorInfo.Builder error =
                ErrorInfo.newBuilder().setCode(failure.code()).setMessage(failure.message());
        if (failure.hresult().isPresent()) {
            error.setHresult(failure.hresult().getAsInt());
        }
        return error.build();
    }
}
