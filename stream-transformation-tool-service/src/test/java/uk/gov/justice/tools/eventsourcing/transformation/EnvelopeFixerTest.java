package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.Optional.empty;
import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;

import uk.gov.justice.services.common.converter.ObjectToJsonValueConverter;
import uk.gov.justice.services.common.converter.jackson.ObjectMapperProducer;
import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.TransformingEnveloper;

import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class EnvelopeFixerTest {

    @Spy
    private Enveloper enveloper = createEnveloper();


    @InjectMocks
    private EnvelopeFixer envelopeFixer;


    @Test
    public void shouldRemoveThePositionAndGiveANewIdUsingTheEnveloper() throws Exception {

        final UUID id = randomUUID();
        final String commandName = "some-event";

        final JsonEnvelope envelope = envelopeFrom(
                metadataBuilder()
                        .withId(id)
                        .withName(commandName)
                        .withVersion(23L),
                createObjectBuilder()
                        .add("exampleField", "example value"));

        final JsonEnvelope jsonEnvelope = envelopeFixer.clearPositionAndGiveNewId(envelope);

        assertThat(jsonEnvelope.metadata().version(), is(empty()));
        assertThat(jsonEnvelope.metadata().id(), is(not(id)));
        assertThat(jsonEnvelope.metadata().name(), is(commandName));
    }

    private Enveloper createEnveloper() {

        final ObjectMapper objectMapper = new ObjectMapperProducer().objectMapper();
        final ObjectToJsonValueConverter objectToJsonValueConverter = new ObjectToJsonValueConverter(objectMapper);
        final EnvelopeFactory envelopeFactory = new EnvelopeFactory(objectToJsonValueConverter);
        return new TransformingEnveloper(envelopeFactory);
    }
}
