package uk.gov.justice.tools.eventsourcing.anonymization.util;

import static java.util.Optional.empty;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import java.util.regex.Pattern;


public class MatcherUtil {

    private MatcherUtil() {
    }

    public static void assertStringIsAnonymisedButOfSameLength(String actualValue, String expectedValue) {
        assertStringIsAnonymisedButOfSameLength(actualValue, expectedValue, empty());
    }

    public static void assertStringIsAnonymisedButOfSameLength(String actualValue, String expectedValue, Optional<Pattern> optionalPattern) {
        assertFalse(actualValue.equalsIgnoreCase(expectedValue));
        assertThat(actualValue.length(), is(expectedValue.length()));
        optionalPattern.ifPresent(op -> assertTrue(op.matcher(actualValue).matches()));
    }
}
