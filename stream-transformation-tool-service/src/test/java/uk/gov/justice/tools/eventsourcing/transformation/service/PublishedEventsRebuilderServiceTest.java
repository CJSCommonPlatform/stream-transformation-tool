package uk.gov.justice.tools.eventsourcing.transformation.service;

import static org.mockito.Mockito.verify;

import uk.gov.justice.services.eventsourcing.publishedevent.rebuild.PublishedEventRebuilder;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PublishedEventsRebuilderServiceTest {

    @Mock
    private PublishedEventRebuilder publishedEventRebuilder;

    @InjectMocks
    private PublishedEventsRebuilderService publishedEventsRebuilderService;

    @Test
    public void shouldPopulateLinkedEvents() throws EventStreamException {
        publishedEventsRebuilderService.rebuild();
        verify(publishedEventRebuilder).rebuild();
    }
}