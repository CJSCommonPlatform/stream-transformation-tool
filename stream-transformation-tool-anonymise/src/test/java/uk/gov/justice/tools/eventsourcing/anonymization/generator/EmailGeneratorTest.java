package uk.gov.justice.tools.eventsourcing.anonymization.generator;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.commons.validator.routines.EmailValidator;
import org.junit.Test;

public class EmailGeneratorTest {

    @Test
    public void shouldGenerateARandomEmail() {
        final EmailGenerator emailGenerator = new EmailGenerator();
        final String emailValue = "test@test.com";
        final String anonymisedEmailValue = emailGenerator.convert(emailValue);
        assertTrue(EmailValidator.getInstance().isValid(anonymisedEmailValue));
        assertThat(anonymisedEmailValue, endsWith("@mail.com"));
        assertThat(anonymisedEmailValue.length(), is(emailValue.length()));
    }
}
