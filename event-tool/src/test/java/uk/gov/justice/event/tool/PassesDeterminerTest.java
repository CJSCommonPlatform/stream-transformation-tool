package uk.gov.justice.event.tool;

import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import uk.gov.justice.tools.eventsourcing.transformation.EventTransformationRegistry;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PassesDeterminerTest {

    @Mock
    private EventTransformationRegistry eventTransformationRegistry;

    @InjectMocks
    private PassesDeterminer passesDeterminer;

    @Test
    public void shouldReturnTrueIfLastElementFound() throws Exception {
        when(eventTransformationRegistry.getPasses()).thenReturn(newHashSet(1, 2));

        passesDeterminer.getNextPassValue();

        assertTrue(passesDeterminer.isLastElementInPasses());
    }

    @Test
    public void shouldIncrementPassValue() throws Exception {
        assertThat(passesDeterminer.getPassValue(), is(1));
        assertThat(passesDeterminer.getNextPassValue(), is(2));
        assertThat(passesDeterminer.getNextPassValue(), is(3));
        assertThat(passesDeterminer.passCount(), is(0));
    }
}