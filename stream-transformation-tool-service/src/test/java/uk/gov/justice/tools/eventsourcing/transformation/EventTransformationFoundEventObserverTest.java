package uk.gov.justice.tools.eventsourcing.transformation;


import static org.mockito.Mockito.verify;

import uk.gov.justice.tools.eventsourcing.transformation.api.extension.EventTransformationFoundEvent;
import uk.gov.justice.tools.eventsourcing.transformation.service.EventStreamTransformationServiceTest;

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

        final EventTransformationFoundEvent eventTransformationEvent = new EventTransformationFoundEvent(EventStreamTransformationServiceTest.TestTransformation.class, 1);

        eventTransformationFoundEventObserver.register(eventTransformationEvent);

        verify(logger).info("Loading Event Transformation TestTransformation");
        verify(eventTransformationRegistry).createTransformations(eventTransformationEvent);
    }
}