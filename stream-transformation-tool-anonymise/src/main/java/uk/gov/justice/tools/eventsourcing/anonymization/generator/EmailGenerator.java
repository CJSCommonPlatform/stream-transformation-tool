package uk.gov.justice.tools.eventsourcing.anonymization.generator;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

public class EmailGenerator extends Generator<String> {

    @Override
    public String convert(String fieldValue) {
        return randomAlphanumeric(fieldValue.length() - 9) + "@mail.com";
    }
}
