package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.services.test.utils.core.matchers.UuidStringMatcher.isAUuid;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.time.ZonedDateTime;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.runners.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class EventPositionClearerTest {

    @InjectMocks
    private EventPositionClearer eventPositionClearer;

    @Test
    public void shouldRemoveThePositionFromAnEventAndGiveANewId() throws Exception {

        final UUID correlationId = randomUUID();
        final UUID sessionId = randomUUID();
        final UUID userId = randomUUID();
        final UUID streamId = randomUUID();
        final UUID id = randomUUID();
        final String commandName = "some-command";

        final int version = 23;
        final ZonedDateTime createdAt = new UtcClock().now();

        final JsonEnvelope envelope = envelopeFrom(
                metadataBuilder()
                        .withId(id)
                        .withName(commandName)
                        .withClientCorrelationId(correlationId.toString())
                        .withSessionId(sessionId.toString())
                        .withUserId(userId.toString())
                        .withStreamId(streamId)
                        .createdAt(createdAt)
                .withVersion(version),

                createObjectBuilder()
                        .add("exampleField", "example value"));


        final JsonEnvelope newEnvelope = eventPositionClearer.clearEventPositioning(envelope);

        assertThat(newEnvelope.metadata().version(), is(empty()));
        assertThat(newEnvelope.metadata().createdAt(), is(of(createdAt)));

        assertThat(newEnvelope.metadata().id().toString(), isAUuid());
        assertThat(newEnvelope.metadata().id(), is(not(id)));

        assertThat(newEnvelope.metadata().streamId(), is(of(streamId)));
        assertThat(newEnvelope.metadata().name(), is(commandName));
        assertThat(newEnvelope.metadata().clientCorrelationId(), is(of(correlationId.toString())));
        assertThat(newEnvelope.metadata().sessionId(), is(of(sessionId.toString())));
        assertThat(newEnvelope.metadata().userId(), is(of(userId.toString())));

        assertThat(newEnvelope.payload(), is(envelope.payload()));
    }

    @Test
    public void shouldHandleOptionalValues() throws Exception {

        final UUID id = randomUUID();
        final String commandName = "notification-added";

        final int version = 23;

        final JsonEnvelope envelope = envelopeFrom(
                metadataBuilder()
                        .withId(id)
                        .withName(commandName)
                        .withVersion(version),

                createObjectBuilder()
                        .add("exampleField", "example value"));


        final JsonEnvelope newEnvelope = eventPositionClearer.clearEventPositioning(envelope);

        assertThat(newEnvelope.metadata().version(), is(empty()));
        assertThat(newEnvelope.metadata().createdAt(), is(empty()));

        assertThat(newEnvelope.metadata().id().toString(), isAUuid());
        assertThat(newEnvelope.metadata().id(), is(not(id)));

        assertThat(newEnvelope.metadata().name(), is(commandName));

        assertThat(newEnvelope.metadata().streamId(), is(empty()));
        assertThat(newEnvelope.metadata().clientCorrelationId(), is(empty()));
        assertThat(newEnvelope.metadata().sessionId(), is(empty()));
        assertThat(newEnvelope.metadata().userId(), is(empty()));

        assertThat(newEnvelope.payload(), is(envelope.payload()));
    }
}
