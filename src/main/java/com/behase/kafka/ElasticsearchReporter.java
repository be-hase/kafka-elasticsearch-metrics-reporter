package com.behase.kafka;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;
import lombok.Cleanup;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ElasticsearchReporter extends AbstractPollingReporter implements MetricProcessor<DateTime> {
	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchReporter.class);

	public static final MetricPredicate DEFAULT_METRIC_PREDICATE = MetricPredicate.ALL;
	public static final String DEFAULT_INDEX_PREFIX = "elasticsearch-reporter-default";
	public static final String DEFAULT_TIMESTAMP_FIELD_NAME = "@timestamp";
	public static final String DEFAULT_NAME = "elasticsearch-reporter";

	protected static final String ES_BULK_INDEX_RAW_FORMAT = "{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\"}}";
	protected static final String ES_BULK_INDEX_WITH_TTL_RAW_FORMAT = "{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_ttl\":\"%s\"}}";

	protected final String nodes;
	protected final MetricPredicate predicate;
	protected final String indexPrefix;
	protected final String timestampFieldName;
	protected final String ttl;
	protected final boolean printVmMetrics;

	protected final List<String> nodesList = new ArrayList<String>();
	protected StringWriter buffer = new StringWriter();
	protected AtomicInteger nextHostIndex = new AtomicInteger();
	protected String hostname;
	protected final JsonFactory jsonFactory = new JsonFactory();
	protected final VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();

	public ElasticsearchReporter(MetricsRegistry registry, String nodes, MetricPredicate predicate, String indexPrefix,
			String timestampFieldName, String ttl, boolean printVmMetrics, String name) {
		super(registry, name == null ? DEFAULT_NAME : name);

		this.nodes = nodes;
		this.predicate = predicate == null ? DEFAULT_METRIC_PREDICATE : predicate;
		this.indexPrefix = indexPrefix == null ? DEFAULT_INDEX_PREFIX : indexPrefix;
		this.timestampFieldName = timestampFieldName == null ? DEFAULT_TIMESTAMP_FIELD_NAME : timestampFieldName;
		this.ttl = ttl;
		this.printVmMetrics = printVmMetrics;

		String[] nodesArray = nodes.split(",");
		for (String node : nodesArray) {
			nodesList.add(node.trim());
		}

		hostname = "unknown";
		try {
			InetAddress inetAddress = InetAddress.getLocalHost();
			hostname = inetAddress.getCanonicalHostName();
			if (hostname == null || hostname.length() == 0) {
				hostname = inetAddress.getHostAddress();
			}
		} catch (Exception e) {
		}
		hostname = replaceSpecialChars(hostname);
	}

	@Override
	public void run() {
		DateTime epoch = DateTime.now();
		printRegularMetrics(epoch);
		if (printVmMetrics) {
			try {
				printVmMetrics(epoch);
			} catch (Exception ignored) {
				LOG.error("Error printing vm metrics:", ignored);
			}
		}
		sendBulkRequest();
	}

	@Override
	public void processMeter(MetricName metricName, Metered metered, DateTime epoch) throws Exception {
		@Cleanup StringWriter writer = new StringWriter();
		@Cleanup JsonGenerator json = createAndInitJsonGenerator(writer, metricName, epoch);

		json.writeNumberField("m1_rate", metered.oneMinuteRate());
		json.writeNumberField("m5_rate", metered.fiveMinuteRate());
		json.writeNumberField("m15_rate", metered.fifteenMinuteRate());
		json.writeNumberField("mean_rate", metered.meanRate());
		json.writeNumberField("count", metered.count());

		json.writeEndObject();
		json.flush();
		addReportBuffer("meter", writer.toString(), epoch);
	}

	@Override
	public void processCounter(MetricName metricName, Counter counter, DateTime epoch) throws Exception {
		@Cleanup StringWriter writer = new StringWriter();
		@Cleanup JsonGenerator json = createAndInitJsonGenerator(writer, metricName, epoch);

		json.writeNumberField("count", counter.count());

		json.writeEndObject();
		json.flush();
		addReportBuffer("counter", writer.toString(), epoch);
	}

	@Override
	public void processHistogram(MetricName metricName, Histogram histogram, DateTime epoch) throws Exception {
		@Cleanup StringWriter writer = new StringWriter();
		@Cleanup JsonGenerator json = createAndInitJsonGenerator(writer, metricName, epoch);

		final Snapshot snapshot = histogram.getSnapshot();

		json.writeNumberField("max", histogram.max());
		json.writeNumberField("mean", histogram.mean());
		json.writeNumberField("min", histogram.min());
		json.writeNumberField("stddev", histogram.stdDev());
		json.writeNumberField("p50", snapshot.getMedian());
		json.writeNumberField("p75", snapshot.get75thPercentile());
		json.writeNumberField("p95", snapshot.get95thPercentile());
		json.writeNumberField("p98", snapshot.get98thPercentile());
		json.writeNumberField("p99", snapshot.get99thPercentile());
		json.writeNumberField("p999", snapshot.get999thPercentile());
		json.writeNumberField("count", histogram.count());
		json.writeNumberField("sum", histogram.sum());

		json.writeEndObject();
		json.flush();
		addReportBuffer("histogram", writer.toString(), epoch);
	}

	@Override
	public void processTimer(MetricName metricName, Timer timer, DateTime epoch) throws Exception {
		@Cleanup StringWriter writer = new StringWriter();
		@Cleanup JsonGenerator json = createAndInitJsonGenerator(writer, metricName, epoch);

		final Snapshot snapshot = timer.getSnapshot();

		json.writeNumberField("max", timer.max());
		json.writeNumberField("mean", timer.mean());
		json.writeNumberField("min", timer.min());
		json.writeNumberField("stddev", timer.stdDev());
		json.writeNumberField("p50", snapshot.getMedian());
		json.writeNumberField("p75", snapshot.get75thPercentile());
		json.writeNumberField("p95", snapshot.get95thPercentile());
		json.writeNumberField("p98", snapshot.get98thPercentile());
		json.writeNumberField("p99", snapshot.get99thPercentile());
		json.writeNumberField("p999", snapshot.get999thPercentile());
		json.writeNumberField("count", timer.count());
		json.writeNumberField("m1_rate", timer.oneMinuteRate());
		json.writeNumberField("m5_rate", timer.fiveMinuteRate());
		json.writeNumberField("m15_rate", timer.fifteenMinuteRate());
		json.writeNumberField("mean_rate", timer.meanRate());

		json.writeEndObject();
		json.flush();
		addReportBuffer("timer", writer.toString(), epoch);
	}

	@Override
	public void processGauge(MetricName metricName, Gauge<?> gauge, DateTime epoch) throws Exception {
		Object value = gauge.value();
		if (value == null) {
			return;
		}
		@Cleanup StringWriter writer = new StringWriter();
		@Cleanup JsonGenerator json = createAndInitJsonGenerator(writer, metricName, epoch);

		if (value instanceof Long) {
			json.writeNumberField("longValue", (Long)value);
		} else if (value instanceof Integer) {
			json.writeNumberField("integerValue", (Integer)value);
		} else if (value instanceof Short) {
			json.writeNumberField("shortValue", (Short)value);
		} else if (value instanceof Double) {
			json.writeNumberField("doubleValue", (Double)value);
		} else if (value instanceof Float) {
			json.writeNumberField("floatValue", (Float)value);
		} else if (value instanceof String) {
			json.writeStringField("stringValue", replaceSpecialChars((String)value));
		} else if (value instanceof Boolean) {
			json.writeBooleanField("booleanValue", (Boolean)value);
		} else {
			// UNSUPPORTED
			return;
		}

		json.writeEndObject();
		json.flush();
		addReportBuffer("gauge", writer.toString(), epoch);
	}

	protected void printRegularMetrics(final DateTime epoch) {
		for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics(
				predicate).entrySet()) {
			for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
				final Metric metric = subEntry.getValue();
				if (metric != null) {
					try {
						metric.processWith(this, subEntry.getKey(), epoch);
					} catch (Exception ignored) {
						LOG.error("Error printing regular metrics:", ignored);
					}
				}
			}
		}
	}

	protected void printVmMetrics(final DateTime epoch) throws Exception {
		@Cleanup StringWriter writer = new StringWriter();
		@Cleanup JsonGenerator json = createAndInitJsonGenerator(writer, "jvm", epoch);

		json.writeNumberField("memory.heap_usage", vm.heapUsage());
		json.writeNumberField("memory.non_heap_usage", vm.nonHeapUsage());
		for (Map.Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
			if (isEmpty(pool.getKey())) {
				continue;
			}
			String key = replaceSpecialChars(pool.getKey());
			json.writeNumberField("memory.memory_pool_usages." + key, pool.getValue());
		}

		json.writeNumberField("daemon_thread_count", vm.daemonThreadCount());
		json.writeNumberField("thread_count", vm.threadCount());
		json.writeNumberField("uptime", vm.uptime());
		json.writeNumberField("fd_usage", vm.fileDescriptorUsage());
		for (Map.Entry<Thread.State, Double> entry : vm.threadStatePercentages().entrySet()) {
			if (entry.getKey() == null || isEmpty(entry.getKey().toString())) {
				continue;
			}
			String key = entry.getKey().toString().toLowerCase();
			key = replaceSpecialChars(key);
			json.writeNumberField("thread_states." + key, entry.getValue());
		}

		for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
			if (isEmpty(entry.getKey())) {
				continue;
			}
			String key = replaceSpecialChars(entry.getKey());
			final String name = "gc." + key;
			json.writeNumberField(name + ".time", entry.getValue().getTime(TimeUnit.MILLISECONDS));
			json.writeNumberField(name + ".runs", entry.getValue().getRuns());
		}

		json.writeEndObject();
		json.flush();
		addReportBuffer("jvm", writer.toString(), epoch);
	}

	protected JsonGenerator createAndInitJsonGenerator(final Writer sw, MetricName metricName, DateTime epoch)
			throws IOException {
		JsonGenerator gen = jsonFactory.createGenerator(sw);
		gen.writeStartObject();
		gen.writeStringField(timestampFieldName, epoch.toString(ISODateTimeFormat.dateTime()));
		gen.writeStringField("@name", replaceSpecialChars(sanitizeMetricName(metricName)));
		gen.writeStringField("hostname", hostname);
		return gen;
	}

	protected JsonGenerator createAndInitJsonGenerator(final Writer sw, String metricName, DateTime epoch)
			throws IOException {
		JsonGenerator gen = jsonFactory.createGenerator(sw);
		gen.writeStartObject();
		gen.writeStringField(timestampFieldName, epoch.toString(ISODateTimeFormat.dateTime()));
		gen.writeStringField("@name", replaceSpecialChars(metricName));
		gen.writeStringField("hostname", hostname);
		return gen;
	}

	protected void addReportBuffer(String type, String json, DateTime epoch) {
		final String index = indexPrefix + epoch.toString("yyyy.MM.dd");

		if (ttl != null && ttl.length() > 0) {
			buffer.append(String.format(ES_BULK_INDEX_WITH_TTL_RAW_FORMAT, index, type, ttl));
		} else {
			buffer.append(String.format(ES_BULK_INDEX_RAW_FORMAT, index, type));
		}
		buffer.append("\n").append(json).append("\n");
	}

	protected void sendBulkRequest() {
		final String sBuf = buffer.toString();
		buffer = new StringWriter();

		if (isEmpty(sBuf)) {
			LOG.info("The metrics is blank");
			return;
		}
		if (LOG.isDebugEnabled()) {
			LOG.info("=========== Elasticsearch '/_bulk' \n{}", sBuf);
		}

		HttpURLConnection connection = null;
		boolean connected = false;

		for (int i = 0; i < nodesList.size(); i++) { // Round-Robin
			int hostIndex = nextHostIndex.get();
			nextHostIndex.set((nextHostIndex.get() == nodesList.size() - 1) ? 0 : nextHostIndex.get() + 1);
			try {
				URL templateUrl = new URL("http://" + nodesList.get(hostIndex) + "/_bulk");
				LOG.info("Request to Elasticsearch '{}'", templateUrl);
				connection = (HttpURLConnection)templateUrl.openConnection();
				connection.setRequestMethod("POST");
				connection.setConnectTimeout(3000); //3sec
				connection.setUseCaches(false);
				connection.setDoOutput(true);
				connection.connect();
				connected = true;
				break;
			} catch (IOException e) {
				LOG.error("Error connecting to {}: {}", nodesList.get(hostIndex), e);
			}
		}

		if (connected) {
			try {
				OutputStreamWriter printWriter = new OutputStreamWriter(connection.getOutputStream(), "UTF-8");
				printWriter.write(sBuf);
				printWriter.flush();
				printWriter.close();
				closeConnection(connection);

			} catch (Exception e) {
				LOG.warn("Fail! The metric reporting to Elasticsearch.", e);
			}
		}
	}

	protected void closeConnection(HttpURLConnection connection)
			throws IOException {
		connection.getOutputStream().close();
		connection.disconnect();
		if (connection.getResponseCode() != 200) {
			LOG.warn("Reporting returned code {} {}",
					connection.getResponseCode(),
					connection.getResponseMessage());
		}
	}

	public static boolean isEmpty(String str) {
		return str == null || str.length() == 0;
	}

	public static String sanitizeMetricName(MetricName metricName) {
		final StringBuilder nameBuilder = new StringBuilder();
		nameBuilder.append(metricName.getGroup());
		nameBuilder.append(":type=");
		nameBuilder.append(metricName.getType());
		if (metricName.hasScope()) {
			nameBuilder.append(",scope=");
			nameBuilder.append(metricName.getScope());
		}
		nameBuilder.append(",name=");
		nameBuilder.append(metricName.getName());
		return nameBuilder.toString();
	}

	public static String replaceSpecialChars(String str) {
		str = str.replace(" ", "_");
		str = str.replace("-", "_");
		return str;
	}
}
