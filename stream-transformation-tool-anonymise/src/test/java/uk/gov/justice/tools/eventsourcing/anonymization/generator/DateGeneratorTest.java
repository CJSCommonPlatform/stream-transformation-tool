package uk.gov.justice.tools.eventsourcing.anonymization.generator;

import org.apache.commons.validator.routines.DateValidator;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class DateGeneratorTest {

    @Test
    public void shouldGenerateDateWithDayAndMonthAsOne() {
        final DateGenerator dateGenerator = new DateGenerator();
        final String dateValue = "2017-05-19";
        final String anonymisedDateValue = dateGenerator.convert(dateValue);

        assertTrue(DateValidator.getInstance().isValid(anonymisedDateValue,"yyyy-01-01"));
    }
}
