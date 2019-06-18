package uk.gov.justice.tools.eventsourcing.anonymization.generator;


public class DateTimeGenerator extends Generator<String> {

    @Override
    public String convert(final String fieldValue) {
        return fieldValue;
    }
}
