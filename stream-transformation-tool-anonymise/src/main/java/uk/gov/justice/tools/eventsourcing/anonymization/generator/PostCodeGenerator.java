package uk.gov.justice.tools.eventsourcing.anonymization.generator;

public class PostCodeGenerator extends Generator<String> {

    @Override
    public String convert(final String fieldValue) {

        return fieldValue
                .replaceAll("[a-zA-Z]", "A")
                .replaceAll("[0-9]", "1");
    }
}
