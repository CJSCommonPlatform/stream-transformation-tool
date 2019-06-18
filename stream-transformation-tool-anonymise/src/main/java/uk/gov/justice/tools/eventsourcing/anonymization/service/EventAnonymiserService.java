package uk.gov.justice.tools.eventsourcing.anonymization.service;


import static com.google.common.base.Joiner.on;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyList;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import static javax.json.JsonValue.ValueType.ARRAY;
import static javax.json.JsonValue.ValueType.FALSE;
import static javax.json.JsonValue.ValueType.NULL;
import static javax.json.JsonValue.ValueType.NUMBER;
import static javax.json.JsonValue.ValueType.OBJECT;
import static javax.json.JsonValue.ValueType.TRUE;
import static uk.gov.justice.tools.eventsourcing.anonymization.RuleParser.loadAnonymisationRules;

import uk.gov.justice.tools.eventsourcing.anonymization.generator.StringPatternGeneratorFactory;
import uk.gov.justice.tools.eventsourcing.anonymization.model.Event;
import uk.gov.justice.tools.eventsourcing.anonymization.model.Events;

import java.util.List;
import java.util.Optional;

import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventAnonymiserService {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventAnonymiserService.class);

    private static final String OBJECT_ROOT_ELEMENT = "$";
    private static final String ARRAY_ROOT_ELEMENT = "$.[*]";
    private static final List<ValueType> IGNORE_ANONYMISATION_JSON_TYPES = newArrayList(NULL, TRUE, FALSE, NUMBER);
    private static final String EVENT_ANONYMISATION_RULE_FILE = "events-anonymisation-rule.json";
    private static final Events EVENTS = loadAnonymisationRules(EVENT_ANONYMISATION_RULE_FILE);
    private StringPatternGeneratorFactory stringPatternGeneratorFactory = new StringPatternGeneratorFactory();

    public JsonObject anonymiseObjectPayload(final JsonObject payload, final String eventName) {
        final List<String> fieldsToIgnoreForEvent = getFieldsToIgnoreForEvent(eventName);
        if (fieldsToIgnoreForEvent.contains(OBJECT_ROOT_ELEMENT)) {
            return payload;
        }

        return processObjectPayload(payload, createObjectBuilder(), OBJECT_ROOT_ELEMENT, fieldsToIgnoreForEvent).build();
    }

    public JsonArray anonymiseArrayPayload(final JsonArray payload, final String eventName) {
        final List<String> fieldsToIgnoreForEvent = getFieldsToIgnoreForEvent(eventName);

        if (fieldsToIgnoreForEvent.contains(ARRAY_ROOT_ELEMENT)) {
            return payload;
        }
        return processArrayPayload(payload, createArrayBuilder(), ARRAY_ROOT_ELEMENT, fieldsToIgnoreForEvent).build();
    }


    private JsonObjectBuilder processObjectPayload(final JsonObject payload, final JsonObjectBuilder builder, final String jsonPath, final List<String> fieldsToIgnore) {

        for (final String fieldName : payload.keySet()) {

            final JsonValue jsonValue = payload.get(fieldName);
            final ValueType fieldValueType = jsonValue.getValueType();
            LOGGER.debug("Field '{}' is of type '{}'", fieldName, fieldValueType);

            if (fieldValueType.equals(OBJECT)) {
                final String pathForField = getPathForField(jsonPath, fieldName);
                if (!fieldsToIgnore.contains(pathForField)) {
                    builder.add(fieldName, processObjectPayload((JsonObject) jsonValue, createObjectBuilder(), pathForField, fieldsToIgnore));
                } else {
                    builder.add(fieldName, payload);
                }
            } else if (fieldValueType.equals(ARRAY)) {
                final String pathForField = getPathForField(jsonPath, fieldName + "[*]");
                final JsonArray jsonArray = ((JsonArray) jsonValue);

                if (!fieldsToIgnore.contains(pathForField)) {
                    builder.add(fieldName, processArrayPayload(jsonArray, createArrayBuilder(), pathForField, fieldsToIgnore));
                } else {
                    builder.add(fieldName, jsonArray);
                }
            } else {
                final String pathForField = getPathForField(jsonPath, fieldName);

                final boolean anonymise = !fieldsToIgnore.contains(pathForField);
                setFieldValueInObject(fieldName, jsonValue, builder, anonymise);
            }
        }
        return builder;

    }

    private JsonArrayBuilder processArrayPayload(final JsonArray payload, final JsonArrayBuilder builder, final String jsonPath, final List<String> fieldsToIgnore) {


        for (int counter = 0; counter < payload.size(); counter++) {
            final JsonValue value = payload.get(counter);
            final ValueType fieldValueType = value.getValueType();
            final String pathForField = getPathForField(jsonPath, "[*]");

            if (fieldValueType.equals(OBJECT)) {
                if (!fieldsToIgnore.contains(pathForField)) {
                    builder.add(processObjectPayload((JsonObject) value, createObjectBuilder(), jsonPath, fieldsToIgnore));
                } else {
                    builder.add(value);
                }
            } else if (fieldValueType.equals(ARRAY)) {
                if (!fieldsToIgnore.contains(pathForField)) {
                    builder.add(processArrayPayload((JsonArray) value, createArrayBuilder(), jsonPath, fieldsToIgnore));
                } else {
                    builder.add(value);
                }
            } else {
                final boolean anonymise = !fieldsToIgnore.contains(pathForField);
                setFieldValueInArray(value, builder, anonymise);
            }
        }
        return builder;

    }


    private void setFieldValueInObject(final String fieldName, final JsonValue value, final JsonObjectBuilder builder, final boolean anonymise) {
        final ValueType fieldValueType = value.getValueType();
        if (IGNORE_ANONYMISATION_JSON_TYPES.contains(fieldValueType) || !anonymise) {
            builder.add(fieldName, value);
            return;
        }

        builder.add(fieldName, getTransformedStringValue(value));
    }

    private void setFieldValueInArray(final JsonValue value, final JsonArrayBuilder builder, final boolean anonymise) {
        final ValueType fieldValueType = value.getValueType();
        if (IGNORE_ANONYMISATION_JSON_TYPES.contains(fieldValueType) || !anonymise) {
            builder.add(value);
            return;
        }

        builder.add(getTransformedStringValue(value));
    }

    private String getTransformedStringValue(final JsonValue value) {
        final String valueAsString = ((JsonString) value).getString();
        return (String) stringPatternGeneratorFactory.getGenerator(valueAsString).convert(valueAsString);
    }

    private List<String> getFieldsToIgnoreForEvent(final String eventName) {
        final Optional<Event> optionalEvent = EVENTS.getEvents().stream().filter(e -> e.getEventName().equals(eventName)).findFirst();
        return optionalEvent.isPresent() ? optionalEvent.get().getFieldsToBeIgnored() : emptyList();
    }

    private String getPathForField(final String rootPath, final String pathSelector) {
        return on(".").join(rootPath, pathSelector);
    }
}
