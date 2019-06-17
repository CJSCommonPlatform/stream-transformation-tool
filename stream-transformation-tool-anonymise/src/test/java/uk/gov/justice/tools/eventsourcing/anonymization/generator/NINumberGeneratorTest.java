package uk.gov.justice.tools.eventsourcing.anonymization.generator;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import uk.gov.justice.tools.eventsourcing.anonymization.constants.StringPattern;

import org.junit.Test;

public class NINumberGeneratorTest {


    @Test
    public void shouldAnonymiseNiNumber() {

        final String anonymisedNumber = new NINumberGenerator().convert("SC123456A");
        assertThat(anonymisedNumber, startsWith("SC"));
        assertThat(anonymisedNumber, endsWith("D"));
        assertTrue(StringPattern.NI_NUMBER_PATTERN.matcher(anonymisedNumber).matches());
    }
}