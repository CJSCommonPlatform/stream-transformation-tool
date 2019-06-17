package uk.gov.justice.tools.eventsourcing.anonymization.generator;

import static uk.gov.justice.tools.eventsourcing.anonymization.constants.StringPattern.DATE_PATTERN;
import static uk.gov.justice.tools.eventsourcing.anonymization.constants.StringPattern.DATE_TIME_PATTERN;
import static uk.gov.justice.tools.eventsourcing.anonymization.constants.StringPattern.EMAIL_PATTERN;
import static uk.gov.justice.tools.eventsourcing.anonymization.constants.StringPattern.NI_NUMBER_PATTERN;
import static uk.gov.justice.tools.eventsourcing.anonymization.constants.StringPattern.POSTCODE_PATTERN;
import static uk.gov.justice.tools.eventsourcing.anonymization.constants.StringPattern.UUID_PATTERN;

import java.util.regex.Pattern;

public class StringPatternGeneratorFactory {

    public Generator getGenerator(String value) {

        if (valueMatchesPattern(value, UUID_PATTERN)) {
            return new UUIDGenerator();
        }

        if (valueMatchesPattern(value, DATE_PATTERN)) {
            return new DateGenerator();
        }

        if (valueMatchesPattern(value, DATE_TIME_PATTERN)) {
            return new DateTimeGenerator();
        }

        if (valueMatchesPattern(value, EMAIL_PATTERN)) {
            return new EmailGenerator();
        }

        if (valueMatchesPattern(value, NI_NUMBER_PATTERN)) {
            return new NINumberGenerator();
        }

        if (valueMatchesPattern(value, POSTCODE_PATTERN)) {
            return new PostCodeGenerator();
        }

        return new SimpleStringGenerator();
    }

    private boolean valueMatchesPattern(final String value, final Pattern pattern) {
        return pattern.matcher(value).matches();
    }

}
