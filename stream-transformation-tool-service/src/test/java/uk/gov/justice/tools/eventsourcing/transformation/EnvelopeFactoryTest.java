package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.services.test.utils.core.matchers.UuidStringMatcher.isAUuid;

import uk.gov.justice.services.common.converter.ObjectToJsonValueConverter;
import uk.gov.justice.services.common.converter.jackson.ObjectMapperProducer;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;

import java.time.ZonedDateTime;
import java.util.UUID;

import javax.json.JsonObject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class EnvelopeFactoryTest {

    @Spy
    private ObjectToJsonValueConverter objectToJsonValueConverter = new ObjectToJsonValueConverter(new ObjectMapperProducer().objectMapper());

    @InjectMocks
    private EnvelopeFactory envelopeFactory;

    @Test
    public void shouldCreateANewEnvelopeWithTheSameValuesNewIdAndNoVersion() throws Exception {

        final UUID correlationId = randomUUID();
        final UUID sessionId = randomUUID();
        final UUID userId = randomUUID();
        final UUID streamId = randomUUID();
        final UUID id = randomUUID();
        final String commandName = "some-command";

        final int version = 23;
        final ZonedDateTime createdAt = new UtcClock().now();

        final UUID causation_1 = randomUUID();
        final UUID causation_2 = randomUUID();

        final Metadata metadata = metadataBuilder()
                .withId(id)
                .withName(commandName)
                .withClientCorrelationId(correlationId.toString())
                .withSessionId(sessionId.toString())
                .withUserId(userId.toString())
                .withStreamId(streamId)
                .createdAt(createdAt)
                .withVersion(version)
                .withCausation(causation_1, causation_2)
                .build();

        final JsonObject payload = createObjectBuilder()
                .add("exampleField", "example value")
                .build();

        final JsonEnvelope jsonEnvelope = envelopeFactory.createFrom(metadata, payload);

        assertThat(jsonEnvelope.metadata().id().toString(), isAUuid());
        assertThat(jsonEnvelope.metadata().id(), is(not(id)));

        assertThat(jsonEnvelope.metadata().version(), is(empty()));
        assertThat(jsonEnvelope.metadata().name(), is(commandName));

        assertThat(jsonEnvelope.metadata().createdAt(), is(of(createdAt)));
        
        assertThat(jsonEnvelope.metadata().streamId(), is(of(streamId)));
        assertThat(jsonEnvelope.metadata().userId(), is(of(userId.toString())));
        assertThat(jsonEnvelope.metadata().sessionId(), is(of(sessionId.toString())));
        assertThat(jsonEnvelope.metadata().clientCorrelationId(), is(of(correlationId.toString())));
        assertThat(jsonEnvelope.metadata().causation(), hasItems(causation_1, causation_2));
    }

    @Test
    public void shouldCreateANewEnvelopeWithTheSameValuesNewNameIdAndNoVersion() throws Exception {

        final UUID correlationId = randomUUID();
        final UUID sessionId = randomUUID();
        final UUID userId = randomUUID();
        final UUID streamId = randomUUID();
        final UUID id = randomUUID();
        final String oldCommandName = "old-command-name";
        final String newCommandName = "new-command-name";

        final int version = 23;
        final ZonedDateTime createdAt = new UtcClock().now();

        final UUID causation_1 = randomUUID();
        final UUID causation_2 = randomUUID();

        final Metadata metadata = metadataBuilder()
                .withId(id)
                .withName(oldCommandName)
                .withClientCorrelationId(correlationId.toString())
                .withSessionId(sessionId.toString())
                .withUserId(userId.toString())
                .withStreamId(streamId)
                .createdAt(createdAt)
                .withVersion(version)
                .withCausation(causation_1, causation_2)
                .build();

        final JsonObject payload = createObjectBuilder()
                .add("exampleField", "example value")
                .build();

        final JsonEnvelope jsonEnvelope = envelopeFactory.createFrom(metadata, newCommandName, payload);

        assertThat(jsonEnvelope.metadata().id().toString(), isAUuid());
        assertThat(jsonEnvelope.metadata().id(), is(not(id)));

        assertThat(jsonEnvelope.metadata().version(), is(empty()));
        assertThat(jsonEnvelope.metadata().name(), is(newCommandName));

        assertThat(jsonEnvelope.metadata().createdAt(), is(of(createdAt)));

        assertThat(jsonEnvelope.metadata().streamId(), is(of(streamId)));
        assertThat(jsonEnvelope.metadata().userId(), is(of(userId.toString())));
        assertThat(jsonEnvelope.metadata().sessionId(), is(of(sessionId.toString())));
        assertThat(jsonEnvelope.metadata().clientCorrelationId(), is(of(correlationId.toString())));
        assertThat(jsonEnvelope.metadata().causation(), hasItems(causation_1, causation_2));
    }
}
