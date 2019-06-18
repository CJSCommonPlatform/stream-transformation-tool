package uk.gov.justice.tools.eventsourcing.anonymization.generator;

import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

public class SimpleStringGeneratorTest {

    @Test
    public void shouldGenerateRandomString() {
        final SimpleStringGenerator stringValueGenerator = new SimpleStringGenerator();
        final String originalValue = "test `string";
        final String anonymisedString = stringValueGenerator.convert(originalValue);
        assertFalse(anonymisedString.equalsIgnoreCase(originalValue));
        assertThat(anonymisedString.length(), CoreMatchers.is(originalValue.length()));
    }

    @Test
    public void shouldGenerateNullValue() {
        final SimpleStringGenerator stringValueGenerator = new SimpleStringGenerator();
        final String originalValue = null;
        final String anonymisedString = stringValueGenerator.convert(originalValue);
        assertNull(anonymisedString);
    }

    @Test
    public void shouldGenerateEmptyStringValue() {
        final SimpleStringGenerator stringValueGenerator = new SimpleStringGenerator();
        final String originalValue = "";
        final String anonymisedString = stringValueGenerator.convert(originalValue);
        assertThat(anonymisedString, isEmptyString());
    }
}
