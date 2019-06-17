package uk.gov.justice.tools.eventsourcing.anonymization.generator;

import static java.util.Optional.of;
import static uk.gov.justice.tools.eventsourcing.anonymization.constants.StringPattern.POSTCODE_PATTERN;
import static uk.gov.justice.tools.eventsourcing.anonymization.util.MatcherUtil.assertStringIsAnonymisedButOfSameLength;

import org.junit.Test;

public class PostCodeGeneratorTest {

    @Test
    public void testPostCodeAnonymisation() {
        PostCodeGenerator generator = new PostCodeGenerator();
        assertStringIsAnonymisedButOfSameLength(generator.convert("M11AE"), "A1AAA", of(POSTCODE_PATTERN));
        assertStringIsAnonymisedButOfSameLength(generator.convert("M1 1AE"), "A1 AAA", of(POSTCODE_PATTERN));
        assertStringIsAnonymisedButOfSameLength(generator.convert("CR05QX"), "A1 AAA", of(POSTCODE_PATTERN));
        assertStringIsAnonymisedButOfSameLength(generator.convert("CR0 5QX"), "AA1 AAA", of(POSTCODE_PATTERN));
        assertStringIsAnonymisedButOfSameLength(generator.convert("SE231QX"), "AA1AAAA", of(POSTCODE_PATTERN));
        assertStringIsAnonymisedButOfSameLength(generator.convert("SE23 1QX"), "AA1A AAA", of(POSTCODE_PATTERN));
    }
}