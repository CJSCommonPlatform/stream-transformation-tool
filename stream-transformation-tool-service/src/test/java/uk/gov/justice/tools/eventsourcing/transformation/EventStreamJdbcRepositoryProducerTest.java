package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.Optional.of;
import static org.junit.Assert.*;

import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepositoryFactory;
import uk.gov.justice.subscription.domain.eventsource.EventSourceDefinition;
import uk.gov.justice.subscription.domain.eventsource.Location;
import uk.gov.justice.subscription.registry.EventSourceDefinitionRegistry;

import java.util.Optional;

import javax.inject.Inject;

@RunWith(MockitoJUnitRunner.class)
public class EventStreamJdbcRepositoryProducerTest {

    @Mock
    private EventSourceDefinitionRegistry eventSourceDefinitionRegistry;

    @Mock
    private EventStreamJdbcRepositoryFactory eventStreamJdbcRepositoryFactory;

    @InjectMocks
    private EventStreamJdbcRepositoryProducer eventStreamJdbcRepositoryProducer;

    @Test
    public void shouldCreateAnEventStreamJdbcRepository() throws Exception {

        final String datasourceJndiName = "the datasource jndi name";

        final EventSourceDefinition defaultEventSourceDefinition = mock(EventSourceDefinition.class);
        final Location location = mock(Location.class);
        final EventStreamJdbcRepository eventStreamJdbcRepository = mock(EventStreamJdbcRepository.class);

        when(eventSourceDefinitionRegistry.getDefaultEventSourceDefinition()).thenReturn(defaultEventSourceDefinition);
        when(defaultEventSourceDefinition.getLocation()).thenReturn(location);
        when(location.getDataSource()).thenReturn(of(datasourceJndiName));

        when(eventStreamJdbcRepositoryFactory.eventStreamJdbcRepository(datasourceJndiName)).thenReturn(eventStreamJdbcRepository);

        assertThat(eventStreamJdbcRepositoryProducer.eventStreamJdbcRepository(), is(eventStreamJdbcRepository));
    }
}
