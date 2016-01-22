package com.behase.kafka;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ElasticsearchReporterTest {
	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchReporterTest.class);

	public static class SpyElasticsearchReporter extends ElasticsearchReporter {
		private static final Logger LOG = LoggerFactory.getLogger(SpyElasticsearchReporter.class);
		private List<String> sendData = new ArrayList<String>();

		public SpyElasticsearchReporter(MetricsRegistry registry, String nodes,
				MetricPredicate predicate,
				String indexPrefix, String timestampFieldName, String ttl, boolean printVmMetrics, String name) {
			super(registry, nodes, predicate, indexPrefix, timestampFieldName, ttl, printVmMetrics, name);
		}

		protected void sendBulkRequest() {
			final String sBuf = buffer.toString();
			buffer = new StringWriter();

			if (sBuf == null || sBuf.length() == 0) {
				LOG.info("!! The metrics is blank !!");
				return;
			}
			if (LOG.isDebugEnabled()) {
				LOG.info("=========== Elasticsearch '/_bulk' \n{}", sBuf);
				sendData = Arrays.asList(sBuf.split("\n"));
			}
		}
	}

	@Test
	public void mockAllTest() throws InterruptedException {
		final MetricsRegistry metrics = new MetricsRegistry();

		final SpyElasticsearchReporter repoter = new SpyElasticsearchReporter(
				metrics,
				"localhost:9200",
				MetricPredicate.ALL,
				"index-",
				null,
				null,
				true,
				null
		);

		repoter.start(1, TimeUnit.SECONDS);

		metrics.newGauge(new MetricName("group", "type", "gauge"), new Gauge<String>() {
			public String value() {
				return "gauge";
			}
		});
		metrics.newCounter(new MetricName("group", "type", "counter")).inc();
		metrics.newMeter(new MetricName("group", "type", "meter"), "meter", TimeUnit.SECONDS).mark();
		metrics.newHistogram(new MetricName("group", "type", "histogram"), false).update(100);
		Timer timer = metrics.newTimer(new MetricName("group", "type", "timer"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
		timer.time();
		timer.stop();

		Thread.sleep(2000);

		LOG.debug("sendData={}", repoter.sendData);
	}
}
