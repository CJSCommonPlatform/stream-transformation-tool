package uk.gov.justice.tools.eventsourcing.transformation;

public class SequenceValidationException extends RuntimeException {
    public SequenceValidationException(final String message) {
        super(message);
    }
}
