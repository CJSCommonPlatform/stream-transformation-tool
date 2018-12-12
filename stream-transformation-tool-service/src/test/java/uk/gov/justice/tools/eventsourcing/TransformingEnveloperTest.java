package uk.gov.justice.tools.eventsourcing;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.tools.eventsourcing.transformation.EnvelopeFactory;

import java.util.function.Function;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TransformingEnveloperTest {

    @Mock
    private EnvelopeFactory envelopeFactory;

    @InjectMocks
    private TransformingEnveloper transformingEnveloper;

    @Test
    public void shouldReturnFunctionToConvertJsonEnvelope() throws Exception {

        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final Object payload = new Object();
        final JsonEnvelope newEnvelope = mock(JsonEnvelope.class);

        when(jsonEnvelope.metadata()).thenReturn(metadata);
        when(envelopeFactory.createFrom(metadata, payload)).thenReturn(newEnvelope);

        final Function<Object, JsonEnvelope> function = transformingEnveloper.withMetadataFrom(jsonEnvelope);

        assertThat(function.apply(payload), is(newEnvelope));
    }

    @Test
    public void shouldReturnFunctionToConvertJsonEnvelopeWithNewName() throws Exception {

        final String name = "a name";

        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final Object payload = new Object();
        final JsonEnvelope newEnvelope = mock(JsonEnvelope.class);

        when(jsonEnvelope.metadata()).thenReturn(metadata);
        when(envelopeFactory.createFrom(metadata, name, payload)).thenReturn(newEnvelope);

        final Function<Object, JsonEnvelope> function = transformingEnveloper.withMetadataFrom(jsonEnvelope, name);

        assertThat(function.apply(payload), is(newEnvelope));
    }
}
