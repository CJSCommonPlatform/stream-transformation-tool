package uk.gov.justice.tools.eventsourcing.anonymization.generator;


import static java.lang.String.format;

import com.mifmif.common.regex.Generex;
import org.apache.commons.lang3.StringUtils;

public class PhoneNumberGenerator extends Generator<String> {

    @Override
    public String convert(final String fieldValue) {
        if (StringUtils.isBlank(fieldValue)) {
            return "";
        }

        return new Generex(format("[0-9]{%s}", fieldValue.length())).random();
    }
}
