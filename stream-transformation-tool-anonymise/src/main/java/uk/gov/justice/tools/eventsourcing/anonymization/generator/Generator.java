package uk.gov.justice.tools.eventsourcing.anonymization.generator;

public abstract class Generator<T> {

    public abstract T convert(final String fieldValue);

}
