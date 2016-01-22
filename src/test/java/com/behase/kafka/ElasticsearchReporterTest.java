package com.behase.kafka;

import com.fasterxml.jackson.core.JsonGenerator;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.stats.Snapshot;
import lombok.Cleanup;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class ElasticsearchReporterTest {
	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchReporterTest.class);
	private static String DOCKER_HOST;

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
	private MetricsRegistry metrics;
	private Configuration jsonConf = Configuration.defaultConfiguration().addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
	private Configuration pathConf = Configuration.defaultConfiguration().addOptions(Option.AS_PATH_LIST);

	@BeforeClass
	public static void beforeClass() {
		DateTime mockDateTime = new DateTime(2016, 1, 1, 0, 0, 0);
		DateTimeUtils.setCurrentMillisFixed(mockDateTime.getMillis());

		DOCKER_HOST = System.getProperty("dockerHost", "192.168.99.100");
	}

	@Before
	public void before() {
		metrics = new MetricsRegistry();

		reporter = new ElasticsearchReporter(
				metrics,
				DOCKER_HOST + ":9200",
				MetricPredicate.ALL,
				"index-",
				null,
				null,
				true,
				null
		);

		spyReporter = new SpyElasticsearchReporter(
				metrics,
				DOCKER_HOST + ":9200",
				MetricPredicate.ALL,
				"index-",
				null,
				null,
				true,
				null
		);
	}

	@Test
	public void processMeter() throws Exception {
		Meter mockMeter = mock(Meter.class);

		doReturn(1.0).when(mockMeter).oneMinuteRate();
		doReturn(5.0).when(mockMeter).fiveMinuteRate();
		doReturn(15.0).when(mockMeter).fifteenMinuteRate();
		doReturn(10.0).when(mockMeter).meanRate();
		doReturn(3l).when(mockMeter).count();

		reporter.processMeter(new MetricName("g", "t", "n"), mockMeter, DateTime.now());

		String[] buf = reporter.buffer.toString().split("\n");

		DocumentContext doc1 = JsonPath.using(jsonConf).parse(buf[0]);
		assertThat(doc1.read("$.index._type", String.class), is("meter"));

		DocumentContext doc2 = JsonPath.using(jsonConf).parse(buf[1]);
		assertThat(doc2.read("$.m1_rate", Double.class), is(1.0));
		assertThat(doc2.read("$.m5_rate", Double.class), is(5.0));
		assertThat(doc2.read("$.m15_rate", Double.class), is(15.0));
		assertThat(doc2.read("$.mean_rate", Double.class), is(10.0));
		assertThat(doc2.read("$.count", Long.class), is(3l));
	}

	@Test
	public void processCounter() throws Exception {
		Counter mockCounter = mock(Counter.class);

		doReturn(1l).when(mockCounter).count();

		reporter.processCounter(new MetricName("g", "t", "n"), mockCounter, DateTime.now());

		String[] buf = reporter.buffer.toString().split("\n");

		DocumentContext doc1 = JsonPath.using(jsonConf).parse(buf[0]);
		assertThat(doc1.read("$.index._type", String.class), is("counter"));

		DocumentContext doc2 = JsonPath.using(jsonConf).parse(buf[1]);
		assertThat(doc2.read("$.count", Integer.class), is(1));
	}

	@Test
	public void processHistogram() throws Exception {
		Histogram mockHistogram = mock(Histogram.class);

		Snapshot mockSnapshot = mock(Snapshot.class);
		doReturn(mockSnapshot).when(mockHistogram).getSnapshot();

		doReturn(100.0).when(mockHistogram).max();
		doReturn(50.0).when(mockHistogram).mean();
		doReturn(0.0).when(mockHistogram).min();
		doReturn(30.0).when(mockHistogram).stdDev();
		doReturn(50.0).when(mockSnapshot).getMedian();
		doReturn(75.0).when(mockSnapshot).get75thPercentile();
		doReturn(95.0).when(mockSnapshot).get95thPercentile();
		doReturn(98.0).when(mockSnapshot).get98thPercentile();
		doReturn(99.0).when(mockSnapshot).get99thPercentile();
		doReturn(999.0).when(mockSnapshot).get999thPercentile();
		doReturn(1l).when(mockHistogram).count();
		doReturn(1000.0).when(mockHistogram).sum();

		reporter.processHistogram(new MetricName("g", "t", "n"), mockHistogram, DateTime.now());

		String[] buf = reporter.buffer.toString().split("\n");

		DocumentContext doc1 = JsonPath.using(jsonConf).parse(buf[0]);
		assertThat(doc1.read("$.index._type", String.class), is("histogram"));

		DocumentContext doc2 = JsonPath.using(jsonConf).parse(buf[1]);
		assertThat(doc2.read("$.max", Double.class), is(100.0));
		assertThat(doc2.read("$.mean", Double.class), is(50.0));
		assertThat(doc2.read("$.min", Double.class), is(0.0));
		assertThat(doc2.read("$.stddev", Double.class), is(30.0));
		assertThat(doc2.read("$.p50", Double.class), is(50.0));
		assertThat(doc2.read("$.p75", Double.class), is(75.0));
		assertThat(doc2.read("$.p95", Double.class), is(95.0));
		assertThat(doc2.read("$.p98", Double.class), is(98.0));
		assertThat(doc2.read("$.p99", Double.class), is(99.0));
		assertThat(doc2.read("$.p999", Double.class), is(999.0));
		assertThat(doc2.read("$.count", Long.class), is(1l));
		assertThat(doc2.read("$.sum", Double.class), is(1000.0));
	}

	@Test
	public void processTimer() throws Exception {
		Timer mockTimer = mock(Timer.class);

		Snapshot mockSnapshot = mock(Snapshot.class);
		doReturn(mockSnapshot).when(mockTimer).getSnapshot();

		doReturn(100.0).when(mockTimer).max();
		doReturn(50.0).when(mockTimer).mean();
		doReturn(0.0).when(mockTimer).min();
		doReturn(30.0).when(mockTimer).stdDev();
		doReturn(50.0).when(mockSnapshot).getMedian();
		doReturn(75.0).when(mockSnapshot).get75thPercentile();
		doReturn(95.0).when(mockSnapshot).get95thPercentile();
		doReturn(98.0).when(mockSnapshot).get98thPercentile();
		doReturn(99.0).when(mockSnapshot).get99thPercentile();
		doReturn(999.0).when(mockSnapshot).get999thPercentile();
		doReturn(1l).when(mockTimer).count();
		doReturn(1.0).when(mockTimer).oneMinuteRate();
		doReturn(5.0).when(mockTimer).fiveMinuteRate();
		doReturn(15.0).when(mockTimer).fifteenMinuteRate();
		doReturn(20.0).when(mockTimer).meanRate();

		reporter.processTimer(new MetricName("g", "t", "n"), mockTimer, DateTime.now());

		String[] buf = reporter.buffer.toString().split("\n");
		LOG.debug("{}", reporter.buffer.toString());

		DocumentContext doc1 = JsonPath.using(jsonConf).parse(buf[0]);
		assertThat(doc1.read("$.index._type", String.class), is("timer"));

		DocumentContext doc2 = JsonPath.using(jsonConf).parse(buf[1]);
		assertThat(doc2.read("$.max", Double.class), is(100.0));
		assertThat(doc2.read("$.mean", Double.class), is(50.0));
		assertThat(doc2.read("$.min", Double.class), is(0.0));
		assertThat(doc2.read("$.stddev", Double.class), is(30.0));
		assertThat(doc2.read("$.p50", Double.class), is(50.0));
		assertThat(doc2.read("$.p75", Double.class), is(75.0));
		assertThat(doc2.read("$.p95", Double.class), is(95.0));
		assertThat(doc2.read("$.p98", Double.class), is(98.0));
		assertThat(doc2.read("$.p99", Double.class), is(99.0));
		assertThat(doc2.read("$.p999", Double.class), is(999.0));
		assertThat(doc2.read("$.count", Long.class), is(1l));
		assertThat(doc2.read("$.m1_rate", Double.class), is(1.0));
		assertThat(doc2.read("$.m5_rate", Double.class), is(5.0));
		assertThat(doc2.read("$.m15_rate", Double.class), is(15.0));
		assertThat(doc2.read("$.mean_rate", Double.class), is(20.0));
	}

	@Test
	public void processGauge_Long() throws Exception {
		Gauge mockGauge = mock(Gauge.class);
		doReturn(1L).when(mockGauge).value();

		reporter.processGauge(new MetricName("g", "t", "n"), mockGauge, DateTime.now());
		String[] buf = reporter.buffer.toString().split("\n");

		DocumentContext doc1 = JsonPath.using(jsonConf).parse(buf[0]);
		assertThat(doc1.read("$.index._type", String.class), is("gauge"));

		DocumentContext doc2 = JsonPath.using(jsonConf).parse(buf[1]);
		assertThat(doc2.read("$.longValue", Long.class), is(1L));
	}

	@Test
	public void processGauge_Integer() throws Exception {
		Gauge mockGauge = mock(Gauge.class);
		doReturn(1).when(mockGauge).value();

		reporter.processGauge(new MetricName("g", "t", "n"), mockGauge, DateTime.now());
		String[] buf = reporter.buffer.toString().split("\n");

		DocumentContext doc = JsonPath.using(jsonConf).parse(buf[1]);
		assertThat(doc.read("$.integerValue", Integer.class), is(1));
	}

	@Test
	public void processGauge_Short() throws Exception {
		Gauge mockGauge = mock(Gauge.class);
		doReturn((short)1).when(mockGauge).value();

		reporter.processGauge(new MetricName("g", "t", "n"), mockGauge, DateTime.now());
		String[] buf = reporter.buffer.toString().split("\n");

		DocumentContext doc = JsonPath.using(jsonConf).parse(buf[1]);
		assertThat(doc.read("$.shortValue", Integer.class), is(1));
	}

	@Test
	public void processGauge_Double() throws Exception {
		Gauge mockGauge = mock(Gauge.class);
		doReturn(1.1).when(mockGauge).value();

		reporter.processGauge(new MetricName("g", "t", "n"), mockGauge, DateTime.now());
		String[] buf = reporter.buffer.toString().split("\n");

		DocumentContext doc = JsonPath.using(jsonConf).parse(buf[1]);
		assertThat(doc.read("$.doubleValue", Double.class), is(1.1));
	}

	@Test
	public void processGauge_Float() throws Exception {
		Gauge mockGauge = mock(Gauge.class);
		doReturn(1.1f).when(mockGauge).value();

		reporter.processGauge(new MetricName("g", "t", "n"), mockGauge, DateTime.now());
		String[] buf = reporter.buffer.toString().split("\n");

		DocumentContext doc = JsonPath.using(jsonConf).parse(buf[1]);
		assertThat(doc.read("$.floatValue", Float.class), is(1.1f));
	}

	@Test
	public void processGauge_String() throws Exception {
		Gauge mockGauge = mock(Gauge.class);
		doReturn("hoge").when(mockGauge).value();

		reporter.processGauge(new MetricName("g", "t", "n"), mockGauge, DateTime.now());
		String[] buf = reporter.buffer.toString().split("\n");

		DocumentContext doc = JsonPath.using(jsonConf).parse(buf[1]);
		assertThat(doc.read("$.stringValue", String.class), is("hoge"));
	}

	@Test
	public void processGauge_Boolean() throws Exception {
		Gauge mockGauge = mock(Gauge.class);
		doReturn(true).when(mockGauge).value();

		reporter.processGauge(new MetricName("g", "t", "n"), mockGauge, DateTime.now());
		String[] buf = reporter.buffer.toString().split("\n");

		DocumentContext doc = JsonPath.using(jsonConf).parse(buf[1]);
		assertThat(doc.read("$.booleanValue", Boolean.class), is(true));
	}

	@Test
	public void printRegularMetrics() {
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

		reporter.printRegularMetrics(DateTime.now());
		assertThat(reporter.buffer.toString().split("\n").length, is(10));
	}

	@Test
	public void printRegularMetrics_with_predicate() {
		reporter = new ElasticsearchReporter(
				metrics,
				DOCKER_HOST + ":9200",
				new ExcludeRegexRegexMetricPredicate("group.type.gauge|group.type.counter"),
				"index-",
				null,
				null,
				true,
				null
		);

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

		reporter.printRegularMetrics(DateTime.now());
		assertThat(reporter.buffer.toString().split("\n").length, is(6));
	}

	@Test
	public void printVmMetrics() throws Exception {
		reporter.printVmMetrics(DateTime.now());
		String[] buf = reporter.buffer.toString().split("\n");

		DocumentContext doc1 = JsonPath.using(jsonConf).parse(buf[0]);
		assertThat(doc1.read("$.index._type", String.class), is("jvm"));

		DocumentContext doc2 = JsonPath.using(pathConf).parse(buf[1]);
		List<String> pathList = doc2.read("$..*");
		assertThat(pathList, hasItems(
				"$['memory.heap_usage']",
				"$['memory.non_heap_usage']",
				"$['daemon_thread_count']",
				"$['thread_count']",
				"$['uptime']",
				"$['daemon_thread_count']",
				"$['fd_usage']"
		));
		assertThat(pathList, hasItems(
				containsString("memory.memory_pool_usages"),
				containsString("thread-states"),
				containsString("gc")
		));
	}

	@Test
	public void createAndInitJsonGenerator_String() throws Exception {
		@Cleanup StringWriter sw = new StringWriter();
		@Cleanup JsonGenerator json = reporter.createAndInitJsonGenerator(sw, "metricName", DateTime.now());
		json.writeEndObject();
		json.flush();

		DocumentContext doc = JsonPath.using(jsonConf).parse(sw.toString());
		assertThat(doc.read("$.@timestamp", String.class), is("2016-01-01T00:00:00.000+09:00"));
		assertThat(doc.read("$.@name", String.class), is("metricName"));
		assertThat(doc.read("$.hostname", String.class), is(reporter.hostname));
	}

	@Test
	public void createAndInitJsonGenerator_MetricName() throws Exception {
		@Cleanup StringWriter sw = new StringWriter();
		@Cleanup JsonGenerator json = reporter.createAndInitJsonGenerator(sw, new MetricName("group", "type", "name"), DateTime.now());
		json.writeEndObject();
		json.flush();

		DocumentContext doc = JsonPath.using(jsonConf).parse(sw.toString());
		assertThat(doc.read("$.@timestamp", String.class), is("2016-01-01T00:00:00.000+09:00"));
		assertThat(doc.read("$.@name", String.class), is("group.type.name"));
		assertThat(doc.read("$.hostname", String.class), is(reporter.hostname));
	}

	@Test
	public void addReportBuffer() {
		reporter.addReportBuffer("type", "{}", DateTime.now());
		String[] buf = reporter.buffer.toString().split("\n");

		DocumentContext doc = JsonPath.using(jsonConf).parse(buf[0]);
		assertThat(doc.read("$.index._index", String.class), is("index-2016.01.01"));
		assertThat(doc.read("$.index._type", String.class), is("type"));
		assertThat(doc.read("$.index._ttl"), is(nullValue()));
		assertThat(buf[1], is("{}"));
	}

	@Test
	public void addReportBuffer_with_ttl() {
		reporter = new ElasticsearchReporter(
				metrics,
				DOCKER_HOST + ":9200",
				MetricPredicate.ALL,
				"index-",
				null,
				"1d",
				true,
				null
		);
		reporter.addReportBuffer("type", "{}", DateTime.now());

		String[] buf = reporter.buffer.toString().split("\n");
		DocumentContext doc = JsonPath.using(jsonConf).parse(buf[0]);
		assertThat(doc.read("$.index._index", String.class), is("index-2016.01.01"));
		assertThat(doc.read("$.index._type", String.class), is("type"));
		assertThat(doc.read("$.index._ttl", String.class), is("1d"));
		assertThat(buf[1], is("{}"));
	}

	@Test
	public void sendBulkRequest() throws Exception {
		@Cleanup CloseableHttpClient httpClient = HttpClients.createDefault();

		HttpDelete deleteMethod = new HttpDelete("http://" + DOCKER_HOST + ":9200/index-2016.01.01");
		httpClient.execute(deleteMethod);
		Thread.sleep(1000);

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

		reporter.printRegularMetrics(DateTime.now());
		reporter.sendBulkRequest();
		Thread.sleep(1000);

		HttpGet getMethod = new HttpGet("http://" + DOCKER_HOST + ":9200/index-2016.01.01/_search");
		@Cleanup CloseableHttpResponse response = httpClient.execute(getMethod);
		HttpEntity entity = response.getEntity();
		String body = EntityUtils.toString(entity, "UTF-8");

		DocumentContext doc = JsonPath.using(jsonConf).parse(body);
		assertThat(doc.read("$.hits.total", Integer.class), is(5));
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
	public void all() throws InterruptedException {
		spyReporter.start(1, TimeUnit.SECONDS);

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

		LOG.debug("sendData={}", spyReporter.sendData);
		assertThat(spyReporter.sendData.size(), is(12));
	}
}
