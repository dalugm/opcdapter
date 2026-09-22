/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

import java.util.Objects;
import java.util.OptionalInt;

import com.dalugm.opcdapter.api.opcda.v1.ErrorCode;

public record ReadResult(String itemId, int quality, long sourceTimestampMs, Outcome outcome) {

    public sealed interface Outcome permits Value, Failure {}

    public record Value(Object value) implements Outcome {
        public Value {
            Objects.requireNonNull(value, "value");
        }
    }

    public record Failure(ErrorCode code, String message, OptionalInt hresult) implements Outcome {
        public Failure {
            Objects.requireNonNull(code, "code");
            Objects.requireNonNull(message, "message");
            Objects.requireNonNull(hresult, "hresult");
        }
    }

    public ReadResult {
        Objects.requireNonNull(itemId, "itemId");
        Objects.requireNonNull(outcome, "outcome");
    }

    public static ReadResult success(
            String itemId, Object value, int quality, long sourceTimestampMs) {
        return new ReadResult(itemId, quality, sourceTimestampMs, new Value(value));
    }

    public static ReadResult failure(
            String itemId, int quality, long sourceTimestampMs, ErrorCode code, String message) {
        return new ReadResult(
                itemId,
                quality,
                sourceTimestampMs,
                new Failure(code, message, OptionalInt.empty()));
    }

    public static ReadResult failure(
            String itemId,
            int quality,
            long sourceTimestampMs,
            ErrorCode code,
            String message,
            int hresult) {
        return new ReadResult(
                itemId,
                quality,
                sourceTimestampMs,
                new Failure(code, message, OptionalInt.of(hresult)));
    }

    public Object value() {
        return outcome instanceof Value success ? success.value() : null;
    }

    public String error() {
        return outcome instanceof Failure failure ? failure.message() : null;
    }

    public ErrorCode errorCode() {
        return outcome instanceof Failure failure
                ? failure.code()
                : ErrorCode.ERROR_CODE_UNSPECIFIED;
    }

    public OptionalInt hresult() {
        return outcome instanceof Failure failure ? failure.hresult() : OptionalInt.empty();
    }

    public boolean isGood() {
        return outcome instanceof Value && (quality & 0xC0) == 0xC0;
    }
}
