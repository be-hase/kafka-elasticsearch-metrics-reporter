package com.behase.kafka;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;

import java.util.regex.Pattern;

public class ExcludeMBeanMetricPredicate implements MetricPredicate {
	Pattern pattern;

	public ExcludeMBeanMetricPredicate(String regex) {
		pattern = Pattern.compile(regex);
	}

	@Override
	public boolean matches(MetricName name, Metric metric) {
		boolean ok = !pattern.matcher(ElasticsearchReporter.sanitizeMetricName(name)).matches();
		return ok;
	}
}
