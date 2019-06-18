package uk.gov.justice.tools.eventsourcing.anonymization.generator;


import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

public class SimpleStringGenerator extends Generator<String> {

    @Override
    public String convert(final String fieldValue) {
        if (null == fieldValue) {
            return null;
        }

        if (StringUtils.isBlank(fieldValue)) {
            return "";
        }

        return RandomStringUtils.randomAlphabetic(fieldValue.length());
    }
}
