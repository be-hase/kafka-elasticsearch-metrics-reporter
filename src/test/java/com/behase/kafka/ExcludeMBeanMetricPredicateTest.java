package com.behase.kafka;

import com.yammer.metrics.core.MetricName;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ExcludeMBeanMetricPredicateTest {
	@Test
	public void matches() {
		MetricName metricName = new MetricName("group", "type", "name", "scope");
		System.out.println(ElasticsearchReporter.sanitizeMetricName(metricName));

		ExcludeMBeanMetricPredicate one = new ExcludeMBeanMetricPredicate("group:.*");
		assertThat(one.matches(metricName, null), is(false));

		ExcludeMBeanMetricPredicate two = new ExcludeMBeanMetricPredicate("name:.*");
		assertThat(two.matches(metricName, null), is(true));
	}
}
