package uk.gov.justice.tools.eventsourcing.transformation;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.getValueOfField;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.tools.eventsourcing.TransformingEnveloper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EnveloperProducerTest {

    @Mock
    private EnvelopeFactory envelopeFactory;

    @InjectMocks
    private EnveloperProducer enveloperProducer;

    @Test
    public void shouldCreateTransformingEnveloper() throws Exception {

        final Enveloper enveloper = enveloperProducer.transformingEnveloper();

        assertThat(enveloper, is(instanceOf(TransformingEnveloper.class)));

        assertThat(getValueOfField(enveloper, "envelopeFactory", EnvelopeFactory.class), is(envelopeFactory));

    }
}
