package uk.gov.justice.tools.eventsourcing.transformation.service;

import static org.mockito.Mockito.verify;

import uk.gov.justice.services.eventsourcing.source.core.LinkedEventSourceTransformation;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LinkedEventStreamTransformationServiceTest {

    @Mock
    private LinkedEventSourceTransformation linkedEventSourceTransformation;

    @InjectMocks
    private LinkedEventStreamTransformationService linkedEventStreamTransformationService;

    @Test
    public void shouldPopulateLinkedEvents() throws EventStreamException {
        linkedEventStreamTransformationService.populateLinkedEvents();
        verify(linkedEventSourceTransformation).populate();
    }

    @Test
    public void shouldTruncateLinkedEvents() throws EventStreamException {
        linkedEventStreamTransformationService.truncateLinkedEvents();
        verify(linkedEventSourceTransformation).truncate();
    }
}