package uk.gov.justice.tools.eventsourcing.anonymization.generator;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static uk.gov.justice.tools.eventsourcing.anonymization.constants.StringPattern.PHONE_NUMBER_PATTERN;
import static uk.gov.justice.tools.eventsourcing.anonymization.util.MatcherUtil.assertStringIsAnonymisedButOfSameLength;

import org.junit.Test;

public class PhoneNumberGeneratorTest {

    @Test
    public void shouldConvertPhoneNumber() {
        final String originalPhoneNumber = "232132";
        final String convertedPhoneNumber = new PhoneNumberGenerator().convert(originalPhoneNumber);
        assertTrue(PHONE_NUMBER_PATTERN.matcher(convertedPhoneNumber).matches());
        assertStringIsAnonymisedButOfSameLength(convertedPhoneNumber, originalPhoneNumber);
    }

    @Test
    public void shouldReturnEmptyStringWhenValueIsNull() {
        assertThat(new PhoneNumberGenerator().convert(null), is(""));
    }

    @Test
    public void shouldReturnEmptyStringWhenValueIsEmptyString() {
        assertThat(new PhoneNumberGenerator().convert(""), is(""));
    }
}