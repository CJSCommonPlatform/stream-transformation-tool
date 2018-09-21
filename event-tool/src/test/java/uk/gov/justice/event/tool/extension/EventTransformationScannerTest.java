package uk.gov.justice.event.tool.extension;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import uk.gov.justice.domain.annotation.Event;
import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.extension.EventTransformationFoundEvent;

import java.util.stream.Stream;

import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.ProcessAnnotatedType;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class EventTransformationScannerTest {

    @Mock
    private ProcessAnnotatedType processAnnotatedType;

    @Mock
    private AnnotatedType annotatedType;

    @Mock
    private BeanManager beanManager;

    @Mock
    private Logger log;

    @InjectMocks
    private EventTransformationScanner eventTransformationScanner;

    @Test
    public void shouldFireEventTransformationFoundEvent() {
        mockProcessAnnotatedType(Transformation.class);
        eventTransformationScanner.processAnnotatedType(processAnnotatedType);
        eventTransformationScanner.afterDeploymentValidation(null, beanManager);

        final ArgumentCaptor<EventTransformationFoundEvent> captor = ArgumentCaptor.forClass(EventTransformationFoundEvent.class);
        verify(beanManager).fireEvent(captor.capture());
        assertThat(captor.getValue(), instanceOf(EventTransformationFoundEvent.class));
    }

    @Test
    public void shouldNotFireEventTransformationFoundEventForNonEventTransformationBeans() {
        mockProcessAnnotatedType(String.class);
        eventTransformationScanner.processAnnotatedType(processAnnotatedType);
        eventTransformationScanner.afterDeploymentValidation(null, beanManager);

        verify(beanManager, never()).fireEvent(any());
    }

    private void mockProcessAnnotatedType(final Class clazz) {
        doReturn(annotatedType).when(processAnnotatedType).getAnnotatedType();
        doReturn(true).when(annotatedType).isAnnotationPresent(clazz);
        doReturn(TestTransformation.class).when(annotatedType).getJavaClass();
        doReturn(mock(Transformation.class)).when(annotatedType).getAnnotation(Transformation.class);
    }

    @Transformation
    public static class TestTransformation implements EventTransformation {

        @Override
        public boolean isApplicable(JsonEnvelope event) {
            return false;
        }

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