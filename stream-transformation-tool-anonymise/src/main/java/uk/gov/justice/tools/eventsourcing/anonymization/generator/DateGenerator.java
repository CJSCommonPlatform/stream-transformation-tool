package uk.gov.justice.tools.eventsourcing.anonymization.generator;


import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DateGenerator extends Generator<String> {

    @Override
    public String convert(final String fieldValue) {
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        final LocalDate date = LocalDate.parse(fieldValue, dateTimeFormatter);
        return date.withMonth(1).withDayOfMonth(1).toString();
    }
}
