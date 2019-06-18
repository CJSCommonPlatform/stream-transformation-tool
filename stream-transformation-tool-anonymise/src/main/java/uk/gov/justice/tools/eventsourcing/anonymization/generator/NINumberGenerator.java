package uk.gov.justice.tools.eventsourcing.anonymization.generator;

import java.util.Random;

public class NINumberGenerator extends Generator<String> {

    @Override
    public String convert(final String fieldValue) {
        final String niNumberPrefix = (null != fieldValue && fieldValue.length() == 9) ? fieldValue.substring(0, 2) : "SC";
        return niNumberPrefix + (new Random().nextInt(900000) + 100000) + "D";
    }
}
