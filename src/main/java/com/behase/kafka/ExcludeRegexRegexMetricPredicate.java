package com.behase.kafka;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;

import java.util.regex.Pattern;

public class ExcludeRegexRegexMetricPredicate implements MetricPredicate {
	Pattern pattern;

	public ExcludeRegexRegexMetricPredicate(String excludeRegex) {
		pattern = Pattern.compile(excludeRegex);
	}

	@Override
	public boolean matches(MetricName name, Metric metric) {
		boolean ok = !pattern.matcher(ElasticsearchReporter.sanitizeName(name)).matches();
		return ok;
	}
}
