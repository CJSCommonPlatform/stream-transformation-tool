package uk.gov.justice.tools.eventsourcing.anonymization.service;

import static java.util.Optional.of;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createReader;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.tools.eventsourcing.anonymization.constants.StringPattern.EMAIL_PATTERN;
import static uk.gov.justice.tools.eventsourcing.anonymization.constants.StringPattern.NI_NUMBER_PATTERN;
import static uk.gov.justice.tools.eventsourcing.anonymization.util.FileUtil.getFileContentsAsString;
import static uk.gov.justice.tools.eventsourcing.anonymization.util.MatcherUtil.assertStringIsAnonymisedButOfLength;
import static uk.gov.justice.tools.eventsourcing.anonymization.util.MatcherUtil.assertStringIsAnonymisedButOfSameLength;

import java.io.StringReader;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.junit.Test;


public class EventAnonymiserServiceTest {

    private EventAnonymiserService service = new EventAnonymiserService();

    @Test
    public void shouldAnonymiseJsonObjectPayload() {
        final JsonObject anonymisedPayload = service.anonymiseObjectPayload(buildObjectPayload("test-object-data.json"), "test-object-event");
        final JsonArray exampleArray = anonymisedPayload.getJsonArray("exampleArray");
        assertThat(exampleArray.getJsonObject(0).getString("attributeIntAsString"), is("001"));
        assertStringIsAnonymisedButOfSameLength(exampleArray.getJsonObject(0).getString("attributeIntAsStringAnonymise"), "001");

        final JsonObject exampleObject = anonymisedPayload.getJsonObject("example");
        assertThat(exampleObject.getString("attributeUUID"), is("9b42e998-158a-4683-8073-8e9453fe6cc9"));
        assertThat(exampleObject.getString("attributeString"), is("Warwick Justice Centre")); // should not anonymise
        assertStringIsAnonymisedButOfLength(exampleObject.getString("attributeStringEmail"), "test123@mail.com", 14, of(EMAIL_PATTERN));
        assertStringIsAnonymisedButOfSameLength(exampleObject.getString("attributeStringNiNumber"), "SC208978A", of(NI_NUMBER_PATTERN));
        assertThat(exampleObject.getString("attributeDate"), is("2017-05-19"));
        assertThat(exampleObject.getJsonArray("attributeArraySimple").getJsonObject(0).getString("arrayAttributeUUID"), is("1905c665-a146-4efc-a01b-d7f035820656"));
        assertThat(exampleObject.getJsonArray("attributeArraySimple").getJsonObject(0).getString("arrayAttributeIntAsString"), is("001"));
        final JsonArray complexArray = exampleObject.getJsonArray("attributeArrayComplex");
        assertStringIsAnonymisedButOfSameLength(complexArray.getString(0), "abc");
        assertThat(complexArray.getInt(1), is(1));
        assertThat(complexArray.getBoolean(2), is(true));
        assertThat(complexArray.getJsonObject(3).getString("arrayAttributeString"), is("abcdef"));
        assertStringIsAnonymisedButOfLength(complexArray.getString(4), "test234@mail.com", 14, of(EMAIL_PATTERN));
        assertStringIsAnonymisedButOfSameLength(complexArray.getString(5), "SC208979B", of(NI_NUMBER_PATTERN));

    }

    @Test
    public void shouldNotAnonymiseJsonObjectPayload() {
        final JsonObject anonymisedPayload = service.anonymiseObjectPayload(buildObjectPayload("test-object-data.json"), "test-object-ignore-all-event");
        final JsonObject exampleObject = anonymisedPayload.getJsonObject("example");
        assertThat(exampleObject.getString("attributeUUID"), is("9b42e998-158a-4683-8073-8e9453fe6cc9"));
        assertThat(exampleObject.getString("attributeString"), is("Warwick Justice Centre"));
        assertThat(exampleObject.getString("attributeStringEmail"), is("test123@mail.com"));
        assertThat(exampleObject.getString("attributeStringNiNumber"), is("SC208978A"));
        assertThat(exampleObject.getString("attributeDate"), is("2017-05-19"));
        assertThat(exampleObject.getJsonArray("attributeArraySimple").getJsonObject(0).getString("arrayAttributeUUID"), is("1905c665-a146-4efc-a01b-d7f035820656"));
        final JsonArray complexArray = exampleObject.getJsonArray("attributeArrayComplex");
        assertThat(complexArray.getString(0), is("abc"));
        assertThat(complexArray.getInt(1), is(1));
        assertThat(complexArray.getBoolean(2), is(true));
        assertThat(complexArray.getJsonObject(3).getString("arrayAttributeString"), is("abcdef"));
        assertThat(complexArray.getString(4), is("test234@mail.com"));
        assertThat(complexArray.getString(5), is("SC208979B"));

    }

    @Test
    public void shouldAnonymiseJsonArrayPayload() {
        final JsonArray anonymisedJsonArray = service.anonymiseArrayPayload(buildArrayPayload("test-array-data.json"), "test-array-event");
        assertStringIsAnonymisedButOfSameLength(anonymisedJsonArray.getString(0), "abc");
        assertThat(anonymisedJsonArray.getInt(1), is(1));
        assertThat(anonymisedJsonArray.getBoolean(2), is(true));
        assertStringIsAnonymisedButOfSameLength(anonymisedJsonArray.getJsonObject(3).getString("stringAttributeAnonymise"), "abcdef");
        assertThat(anonymisedJsonArray.getJsonObject(3).getString("stringAttributeDoNotAnonymise"), is("mnopqr"));
        assertStringIsAnonymisedButOfLength(anonymisedJsonArray.getJsonObject(3).getString("attributeStringEmail"), "test123@mail.com", 14, of(EMAIL_PATTERN));
        assertStringIsAnonymisedButOfSameLength(anonymisedJsonArray.getJsonObject(3).getString("attributeStringNiNumber"), "SC208978A", of(NI_NUMBER_PATTERN));
        assertThat(anonymisedJsonArray.getJsonArray(4), is(createArrayBuilder().add(1).add(2).add(3).build()));
        assertStringIsAnonymisedButOfLength(anonymisedJsonArray.getString(5), "test2345@mail.com", 14, of(EMAIL_PATTERN));
        assertStringIsAnonymisedButOfSameLength(anonymisedJsonArray.getString(6), "SC208979B", of(NI_NUMBER_PATTERN));
    }

    @Test
    public void shouldNotAnonymiseJsonArrayPayload() {
        final JsonArray anonymisedJsonArray = service.anonymiseArrayPayload(buildArrayPayload("test-array-data.json"), "test-array-ignore-all-event");
        assertThat(anonymisedJsonArray.getString(0), is("abc"));
        assertThat(anonymisedJsonArray.getInt(1), is(1));
        assertThat(anonymisedJsonArray.getBoolean(2), is(true));
        assertThat(anonymisedJsonArray.getJsonObject(3).getString("stringAttributeAnonymise"), is("abcdef"));
        assertThat(anonymisedJsonArray.getJsonObject(3).getString("stringAttributeDoNotAnonymise"), is("mnopqr"));
        assertThat(anonymisedJsonArray.getJsonObject(3).getString("attributeStringEmail"), is("test123@mail.com"));
        assertThat(anonymisedJsonArray.getJsonObject(3).getString("attributeStringNiNumber"), is("SC208978A"));
        assertThat(anonymisedJsonArray.getJsonArray(4), is(createArrayBuilder().add(1).add(2).add(3).build()));
        assertThat(anonymisedJsonArray.getString(5), is("test2345@mail.com"));
        assertThat(anonymisedJsonArray.getString(6), is("SC208979B"));
    }


    private JsonObject buildObjectPayload(final String payloadFileName) {
        final JsonReader jsonReader = createReader(new StringReader(getFileContentsAsString(payloadFileName)));
        return jsonReader.readObject();
    }

    private JsonArray buildArrayPayload(final String payloadFileName) {
        final JsonReader jsonReader = createReader(new StringReader(getFileContentsAsString(payloadFileName)));
        return jsonReader.readArray();
    }
}