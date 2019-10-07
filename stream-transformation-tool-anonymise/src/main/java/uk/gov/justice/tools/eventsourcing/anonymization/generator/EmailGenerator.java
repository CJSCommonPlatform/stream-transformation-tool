package uk.gov.justice.tools.eventsourcing.anonymization.generator;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

public class EmailGenerator extends Generator<String> {

    @Override
    public String convert(final String fieldValue) {
        return randomAlphanumeric(5) + "@mail.com";
    }
}
