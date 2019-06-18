package uk.gov.justice.tools.eventsourcing.anonymization.exception;

public class ValidationException extends RuntimeException {

    public ValidationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
