package uk.gov.justice.tools.eventsourcing.anonymization;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static uk.gov.justice.tools.eventsourcing.anonymization.RuleParser.loadAnonymisationRules;

import uk.gov.justice.tools.eventsourcing.anonymization.exception.ValidationException;
import uk.gov.justice.tools.eventsourcing.anonymization.model.Event;
import uk.gov.justice.tools.eventsourcing.anonymization.model.Events;

import org.junit.Test;

public class RuleParserTest {

    @Test
    public void loadValidAnonymisationRules() {

        final Events events = loadAnonymisationRules("data.anonymisation.valid.json");

        assertNotNull(events);
        assertThat(events.getEvents(), hasSize(1));
        final Event firstEvent = events.getEvents().get(0);
        assertThat(firstEvent.getEventName(), is("valid-event"));
        assertThat(firstEvent.getFieldsToBeIgnored(), hasSize(2));
        assertThat(firstEvent.getFieldsToBeIgnored(), hasItems("attributeAJsonPath", "attributeBJsonPath"));
    }

    @Test(expected = ValidationException.class)
    public void loadInvalidAnonymisationRules() {
        loadAnonymisationRules("data.anonymisation.invalid.json");
    }
}