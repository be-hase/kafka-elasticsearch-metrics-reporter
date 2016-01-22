package com.behase.kafka;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricPredicate;
import kafka.metrics.KafkaMetricsConfig;
import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class KafkaElasticsearchMetricsReporter implements KafkaMetricsReporter, KafkaElasticsearchMetricsReporterMBean {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaElasticsearchMetricsReporter.class);

	public static final String DEFAULT_ES_INDEX_PREFIX = "kafka-metrics-";

	protected ElasticsearchReporter reporter;
	protected boolean initialized = false;
	protected boolean running = false;

	protected String esNodes;
	protected String esIndexPrefix;
	protected MetricPredicate predicate;
	protected String esTtl;
	protected boolean getVmInfo;

	@Override
	public void init(VerifiableProperties props) {
		if (!initialized) {
			KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(props);

			esNodes = props.getString("kafka.elasticsearch.metrics.nodes", null);
			esIndexPrefix = props.getString("kafka.elasticsearch.metrics.indexPrefix", DEFAULT_ES_INDEX_PREFIX);
			String predicateRegex = props.getString("kafka.elasticsearch.metrics.excludeRegex", null);
			esTtl = props.getString("kafka.elasticsearch.metrics.ttl", null);
			getVmInfo = props.getBoolean("kafka.elasticsearch.metrics.getVmInfo", true);

			predicate = MetricPredicate.ALL;
			if (predicateRegex != null) {
				predicate = new ExcludeRegexRegexMetricPredicate(predicateRegex);
			}

			// validate
			validate();

			// init
			reporter = new ElasticsearchReporter(
					Metrics.defaultRegistry(),
					esNodes,
					predicate,
					esIndexPrefix,
					null,
					esTtl,
					getVmInfo,
					null
			);

			if (props.getBoolean("kafka.elasticsearch.metrics.reporter.enabled", false)) {
				initialized = true;
				startReporter(metricsConfig.pollingIntervalSecs());
				LOG.debug("KafkaElasticsearchMetricsReporter initialized.");
			}
		}
	}

	public void validate() {
		if (esNodes == null) {
			throw new IllegalArgumentException("kafka.elasticsearch.metrics.nodes is null.");
		}
	}

	@Override
	public void startReporter(long pollingPeriodInSeconds) {
		if (initialized && !running) {
			reporter.start(pollingPeriodInSeconds, TimeUnit.SECONDS);
			running = true;
			LOG.info(String.format("Started KafkaElasticsearchMetricsReporter with polling period %d seconds", pollingPeriodInSeconds));
		}
	}

	@Override
	public void stopReporter() {
		if (initialized && running) {
			reporter.shutdown();
			running = false;
			LOG.info("Stopped KafkaElasticsearchMetricsReporter");

			reporter = new ElasticsearchReporter(
					Metrics.defaultRegistry(),
					esNodes,
					predicate,
					esIndexPrefix,
					null,
					esTtl,
					getVmInfo,
					null
			);
		}
	}

	@Override
	public String getMBeanName() {
		return "kafka:type=com.behase.kafka.KafkaGraphiteMetricsReporter";
	}
}
