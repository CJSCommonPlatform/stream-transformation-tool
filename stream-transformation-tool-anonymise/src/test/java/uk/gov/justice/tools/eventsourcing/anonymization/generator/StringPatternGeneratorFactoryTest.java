package uk.gov.justice.tools.eventsourcing.anonymization.generator;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import org.junit.Before;
import org.junit.Test;

public class StringPatternGeneratorFactoryTest {

    private StringPatternGeneratorFactory factory;

    @Before
    public void setup() {
        factory = new StringPatternGeneratorFactory();
    }


    @Test
    public void shouldReturnDateStringGenerator() {
        assertThat(factory.getGenerator("2019-05-01"), instanceOf(DateGenerator.class));

    }

    @Test
    public void shouldReturnDateTimeStringGenerator() {
        assertThat(factory.getGenerator("2016-11-16T11:22:58.319Z"), instanceOf(DateTimeGenerator.class));
        assertThat(factory.getGenerator("2016-11-16T11:22:58Z"), instanceOf(DateTimeGenerator.class));
        assertThat(factory.getGenerator("2016-11-16T11:22:58"), instanceOf(DateTimeGenerator.class));
        assertThat(factory.getGenerator("2016-11-16 11:22:58.319Z"), instanceOf(DateTimeGenerator.class));
        assertThat(factory.getGenerator("2016-11-16 11:22:58Z"), instanceOf(DateTimeGenerator.class));
        assertThat(factory.getGenerator("2016-11-16 11:22:58"), instanceOf(DateTimeGenerator.class));
        assertThat(factory.getGenerator("2016-11-16T11:22"), instanceOf(DateTimeGenerator.class));
        assertThat(factory.getGenerator("2016-11-16 11:22"), instanceOf(DateTimeGenerator.class));
    }

    @Test
    public void shouldReturnPostCodeStringGenerator() {
        assertThat(factory.getGenerator("S69GT"), instanceOf(PostCodeGenerator.class));
        assertThat(factory.getGenerator("S6 9GT"), instanceOf(PostCodeGenerator.class));
        assertThat(factory.getGenerator("SM69GT"), instanceOf(PostCodeGenerator.class));
        assertThat(factory.getGenerator("SM6 9GT"), instanceOf(PostCodeGenerator.class));
        assertThat(factory.getGenerator("SM6A9GT"), instanceOf(PostCodeGenerator.class));
        assertThat(factory.getGenerator("SM6A 9GT"), instanceOf(PostCodeGenerator.class));
    }

    @Test
    public void shouldReturnUUIDStringGenerator() {
        assertThat(factory.getGenerator(randomUUID().toString()), instanceOf(UUIDGenerator.class));
    }

    @Test
    public void shouldReturnEmailStringGenerator() {
        assertThat(factory.getGenerator("khjkiebckjck@gmail.com"), instanceOf(EmailGenerator.class));
    }

    @Test
    public void shouldReturnNiNumberStringGenerator() {
        assertThat(factory.getGenerator("SC768978A"), instanceOf(NINumberGenerator.class));
    }

}