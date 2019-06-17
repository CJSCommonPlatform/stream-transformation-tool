package uk.gov.justice.tools.eventsourcing.anonymization.generator;


public class DateGenerator extends Generator<String> {

    @Override
    public String convert(String fieldValue) {
        return fieldValue;
    }
}
