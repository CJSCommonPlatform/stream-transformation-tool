package uk.gov.justice.tools.eventsourcing.anonymization.generator;

public class PostCodeGenerator extends Generator<String> {

    @Override
    public String convert(final String fieldValue) {


        switch (fieldValue.length()) {
            case 5:
                return "A11AA";
            case 6:
                return "A1 1AA";
            case 7:
                return "AA1 1AA";
            case 8:
            default:
                return "AA1A 1AA";
        }

    }
}
