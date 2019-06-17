package uk.gov.justice.tools.eventsourcing.anonymization;

import static uk.gov.justice.tools.eventsourcing.anonymization.util.FileUtil.getFileContentsAsString;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

public class SchemaValidatorUtil {

    private SchemaValidatorUtil() {
    }

    public static void validateAgainstSchema(final String schemaFileName, final String jsonString) {
        String schemaPayload = getFileContentsAsString(schemaFileName);
        JSONObject rawSchema = new JSONObject(schemaPayload);
        Schema schema = SchemaLoader.load(rawSchema);
        schema.validate(new JSONObject(jsonString));
    }
}
