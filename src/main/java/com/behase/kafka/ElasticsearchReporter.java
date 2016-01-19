package com.behase.kafka;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;

public class ElasticsearchReporter extends AbstractPollingReporter implements MetricProcessor<Long> {
	private static final Logger LOG = Logger.getLogger(ElasticsearchReporter.class);

	public static final String DEFAULT_NODES = "localhost:9200";
	public static final String DEFAULT_INDEX_PREFIX = "kafka-metrics-default";
	public static final String DEFAULT_TIMESTAMP_FIELD_NAME = "@timestamp";
	public static final int DEFAULT_BULK_LIMIT = 2500;

	private static final String ES_BULK_INDEX_RAW_FORMAT = "{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\"}}";
	private static final String ES_BULK_INDEX_WITH_TTL_RAW_FORMAT = "{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_ttl\":\"%s\"}}";
	private static final DecimalFormat MONTH_AND_DAY_FORMAT = new DecimalFormat("00");

	protected ElasticsearchReporter(MetricsRegistry registry, String name) {
		super(registry, name);
	}

	@Override
	public void run() {

	}

	@Override
	public void processMeter(MetricName metricName, Metered metered, Long aLong) throws Exception {

	}

	@Override
	public void processCounter(MetricName metricName, Counter counter, Long aLong) throws Exception {

	}

	@Override
	public void processHistogram(MetricName metricName, Histogram histogram, Long aLong) throws Exception {

	}

	@Override
	public void processTimer(MetricName metricName, Timer timer, Long aLong) throws Exception {

	}

	@Override
	public void processGauge(MetricName metricName, Gauge<?> gauge, Long aLong) throws Exception {

	}
}
