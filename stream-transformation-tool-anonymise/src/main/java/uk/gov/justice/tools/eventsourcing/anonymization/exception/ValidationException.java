package uk.gov.justice.tools.eventsourcing.anonymization.exception;

public class ValidationException extends RuntimeException {

    public ValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
