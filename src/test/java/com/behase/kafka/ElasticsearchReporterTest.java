package com.behase.kafka;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.OutputCapture;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
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

	private ElasticsearchReporter reporter;
	private SpyElasticsearchReporter spyReporter;

	@Before
	public void setUp() {
		final MetricsRegistry metrics = new MetricsRegistry();

		reporter = new ElasticsearchReporter(
				metrics,
				"localhost:9200",
				MetricPredicate.ALL,
				"index-",
				null,
				1000,
				true,
				null
		);

		spyReporter = new SpyElasticsearchReporter(
				metrics,
				"localhost:9200",
				MetricPredicate.ALL,
				"index-",
				null,
				1000,
				true,
				null
		);
	}

	@Test
	public void closeConnection() throws Exception {
		OutputStream mockOs = mock(OutputStream.class);
		doNothing().when(mockOs).close();

		HttpURLConnection mockConn = mock(HttpURLConnection.class);
		doReturn(mockOs).when(mockConn).getOutputStream();
		doNothing().when(mockConn).disconnect();
		doReturn(400).when(mockConn).getResponseCode();
		doReturn("error message").when(mockConn).getResponseMessage();

		reporter.closeConnection(mockConn);
	}

	@Test
	public void sanitizeName() {
		assertThat(reporter.sanitizeName(new MetricName("group", "type", "name")), is("group.type.name"));
		assertThat(reporter.sanitizeName(new MetricName("group", "type", "name", "scope")), is("group.type.scope.name"));
	}

	@Test
	public void isEmpty() {
		assertThat(ElasticsearchReporter.isEmpty(""), is(true));
		assertThat(ElasticsearchReporter.isEmpty(" "), is(false));
		assertThat(ElasticsearchReporter.isEmpty("a"), is(false));
		assertThat(ElasticsearchReporter.isEmpty(null), is(true));
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
