package com.behase.kafka;

import com.yammer.metrics.core.MetricName;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ExcludeRegexMetricPredicateTest {

	@Test
	public void matches() {
		ExcludeRegexRegexMetricPredicate one = new ExcludeRegexRegexMetricPredicate("group.*");
		assertThat(one.matches(new MetricName("group", "type", "matche"), null), is(false));

		ExcludeRegexRegexMetricPredicate two = new ExcludeRegexRegexMetricPredicate("hoge.*");
		assertThat(two.matches(new MetricName("group", "type", "bar"), null), is(true));
	}
}
