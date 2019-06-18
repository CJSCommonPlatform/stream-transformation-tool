package uk.gov.justice.tools.eventsourcing.anonymization;

import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilderFrom;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.MetadataBuilder;
import uk.gov.justice.services.test.utils.core.enveloper.EnveloperFactory;
import uk.gov.justice.tools.eventsourcing.anonymization.service.EventAnonymiserService;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;

import java.io.IOException;
import java.util.stream.Stream;

import javax.json.JsonObject;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EventAnonymiserTransformationTest {

    private static final String TEST_EVENT_NAME = "test-event";
    private EventAnonymiserTransformation eventAnonymiserTransformation;

    @Mock
    private EventAnonymiserService eventAnonymiserService;

    @Mock
    private JsonEnvelope jsonEnvelope;

    @Mock
    private JsonObject originalPayload;

    @Before
    public void setup() {
        eventAnonymiserTransformation = new EventAnonymiserTransformation(eventAnonymiserService) {
        };
    }


    @Test
    public void shouldAnonymiseJsonObjectPayload() throws IOException {
        eventAnonymiserTransformation.setEnveloper(EnveloperFactory.createEnveloper());
        final JsonObject anonymisedPayload = createObjectBuilder().build();
        when(eventAnonymiserService.anonymiseObjectPayload(originalPayload, TEST_EVENT_NAME)).thenReturn(anonymisedPayload);

        final JsonEnvelope event = buildEnvelope(originalPayload);
        final Stream<JsonEnvelope> jsonEnvelopeStream = eventAnonymiserTransformation.apply(event);

        assertThat(jsonEnvelopeStream.findFirst().get().payloadAsJsonObject(), is(anonymisedPayload));
        verify(eventAnonymiserService).anonymiseObjectPayload(originalPayload, TEST_EVENT_NAME);
    }

    @Test
    public void shouldReturnActionThatIsSuitableForAnonymisation() {
        final Action actionForAnonymisation = eventAnonymiserTransformation.actionFor(jsonEnvelope);
        assertFalse("Should not deactivate stream", actionForAnonymisation.isDeactivate());
        assertFalse("Should not create a back up of the stream", actionForAnonymisation.isKeepBackup());
        assertTrue("Should transform stream", actionForAnonymisation.isTransform());
    }

    private JsonEnvelope buildEnvelope(final JsonObject originalPayload) throws IOException {
        final MetadataBuilder metadataBuilder = metadataBuilderFrom(createObjectBuilder().add("id", randomUUID().toString()).add("name", TEST_EVENT_NAME).build());
        return envelopeFrom(metadataBuilder.withStreamId(randomUUID()).build(), originalPayload);
    }


}
