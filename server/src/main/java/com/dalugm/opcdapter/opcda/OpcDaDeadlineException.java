/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

/** Indicates that an OPC DA operation could not complete within its caller-visible budget. */
public class OpcDaDeadlineException extends RuntimeException {
    public OpcDaDeadlineException(String message) {
        super(message);
    }

    public OpcDaDeadlineException(String message, Throwable cause) {
        super(message, cause);
    }

    public static boolean causedBy(Throwable error) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current instanceof OpcDaDeadlineException) {
                return true;
            }
        }
        return false;
    }
}
