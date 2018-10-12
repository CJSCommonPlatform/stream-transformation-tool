package uk.gov.justice.tools.eventsourcing.transformation;


import static org.mockito.Mockito.verify;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.extension.EventTransformationFoundEvent;

import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class EventTransformationFoundEventObserverTest {


    @Mock
    private Logger logger;

    @InjectMocks
    private EventTransformationFoundEventObserver eventTransformationFoundEventObserver;

    @Mock
    private EventTransformationRegistry eventTransformationRegistry;

    @Test
    public void shouldRegisterTransformation() throws InstantiationException, IllegalAccessException {

        final EventTransformationFoundEvent eventTransformationEvent = new EventTransformationFoundEvent(TestTransformation.class, 1);

        eventTransformationFoundEventObserver.register(eventTransformationEvent);

        verify(logger).info("Loading Event Transformation TestTransformation");
        verify(eventTransformationRegistry).createTransformations(eventTransformationEvent);
    }

    @Transformation
    public static class TestTransformation implements EventTransformation {

        @Override
        public Stream<JsonEnvelope> apply(JsonEnvelope event) {
            return Stream.of(event);
        }

        @Override
        public void setEnveloper(Enveloper enveloper) {
            // Do nothing
        }
    }
}