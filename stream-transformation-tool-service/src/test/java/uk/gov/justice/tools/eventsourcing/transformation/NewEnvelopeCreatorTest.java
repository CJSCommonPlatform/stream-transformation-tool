package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.services.test.utils.core.matchers.UuidStringMatcher.isAUuid;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.MetadataBuilder;

import java.time.ZonedDateTime;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NewEnvelopeCreatorTest {

    @InjectMocks
    private NewEnvelopeCreator newEnvelopeCreator;

    @Test
    public void shouldCreateNewVersionOfTheEnvelopeWithTheOriginalCreatedAtDate() throws Exception {

        final UUID correlationId = randomUUID();
        final UUID sessionId = randomUUID();
        final UUID userId = randomUUID();
        final UUID streamId = randomUUID();
        final UUID id = randomUUID();
        final String commandName = "some-command";

        final long oldVersion = 23L;
        final long newVersion = 29387L;

        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);
        final UUID causationId_1 = randomUUID();
        final UUID causationId_2 = randomUUID();

        final MetadataBuilder metadata = metadataBuilder()
                .withId(id)
                .withName(commandName)
                .withClientCorrelationId(correlationId.toString())
                .withSessionId(sessionId.toString())
                .withUserId(userId.toString())
                .withStreamId(streamId)
                .createdAt(createdAt)
                .withVersion(oldVersion)
                .withCausation(causationId_1, causationId_2);

        final JsonEnvelope event = envelopeFrom(
                metadata,
                createObjectBuilder()
                        .add("exampleField", "example value"));


        final JsonEnvelope envelope = newEnvelopeCreator.toNewEnvelope(event, streamId, newVersion);

        assertThat(envelope.metadata().version(), is(of(newVersion)));
        assertThat(envelope.metadata().createdAt(), is(of(createdAt)));

        assertThat(envelope.metadata().id().toString(), isAUuid());
        assertThat(envelope.metadata().id(), is(not(id)));
        assertThat(envelope.metadata().name(), is(commandName));
        assertThat(envelope.metadata().streamId(), is(of(streamId)));
        assertThat(envelope.metadata().userId(), is(of(userId.toString()
        )));
        assertThat(envelope.metadata().sessionId(), is(of(sessionId.toString())));
        assertThat(envelope.metadata().clientCorrelationId(), is(of(correlationId.toString())));
        assertThat(envelope.metadata().causation(), hasItems(causationId_1, causationId_2));
    }
}
