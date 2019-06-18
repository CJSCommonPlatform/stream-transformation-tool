package uk.gov.justice.tools.eventsourcing.anonymization;


import static uk.gov.justice.tools.eventsourcing.anonymization.SchemaValidatorUtil.validateAgainstSchema;
import static uk.gov.justice.tools.eventsourcing.anonymization.util.FileUtil.getFileContentsAsString;
import static uk.gov.justice.tools.eventsourcing.anonymization.util.FileUtil.getResourceAsStream;

import uk.gov.justice.tools.eventsourcing.anonymization.exception.ValidationException;
import uk.gov.justice.tools.eventsourcing.anonymization.model.Events;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RuleParser {

    private RuleParser() {
    }

    public static Events loadAnonymisationRules(final String ruleFileName) {

        try {
            final String ruleFileSchema = "schema/event-anonymisation-schema.json";
            validateAgainstSchema(ruleFileSchema, getFileContentsAsString(ruleFileName));
            final ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(getResourceAsStream(ruleFileName), Events.class);
        } catch (final Exception e) {
            throw new ValidationException("Error processing json anonymisation rule file: " + ruleFileName, e);
        }
    }
}
