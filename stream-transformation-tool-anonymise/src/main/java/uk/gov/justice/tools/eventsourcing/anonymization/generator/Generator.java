package uk.gov.justice.tools.eventsourcing.anonymization.generator;

public abstract class Generator<T> {

    public Generator() {

    }

    public abstract T convert(String fieldValue);

}
