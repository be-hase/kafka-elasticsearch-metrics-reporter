[![Build Status](https://travis-ci.org/be-hase/kafka-elasticsearch-metrics-reporter.svg?branch=master)](https://travis-ci.org/be-hase/kafka-elasticsearch-metrics-reporter)

# kafka-elasticsearch-metrics-reporter
This is repoter for kafka.  
This send metris data to Elasticseach.

# Requirements
* >= kafka 0.8.2.0
  * Maybe it works 0.8.1.x, but I didn't confirm.
* elasticsearch 
  * I tested with 1.6, 1.7, 2.0, 2.1.

# Install on broker.
1. Build this project using `mvn clean package -DskipTests` or download jar from here.
2. Add `kafka-elasticsearch-metrics-reporter-<version>-shaded.jar` to the `libs/` directory of your kafka broker installation.
3. Configure the broker (see the configuration section below).
4. Restart the broker.

# Configuration

Edit the server.properties file of your installation, activate the reporter by setting:

```
# kafka properties
kafka.metrics.reporters=com.behase.kafka.KafkaElasticsearchMetricsReporter
kafka.metrics.polling.interval.secs=10

# kafka-elasticsearch-metrics-reporter properties
kafka.elasticsearch.metrics.nodes=<elasticsearch-host>:<port>
kafka.elasticsearch.metrics.reporter.enabled=true
```

Here is a list of properties.

| property name | default | required | description |
| --- | --- | --- | --- |
| kafka.elasticsearch.metrics.nodes |  | Y | Host and IP of your elasticsearch nodes. (Comma separated) |
| kafka.elasticsearch.metrics.indexPrefix | kafka-metrics- |  | Prefix of elasticsearch index. |
| kafka.elasticsearch.metrics.excludeRegex |  |  | This can be use to exclude some metrics from graphite.<br>Because kafka has a lot of metrics, it is useful. |
| kafka.elasticsearch.metrics.ttl |  |  | TTL (time to live) |
| kafka.elasticsearch.metrics.getVmInfo | true |  | If this is true, you can get JVM metrics. |
| kafka.elasticsearch.metrics.reporter.enabled | false |  | If you want to use kafka-elasticsearch-metrics-reporter,  set true.|

# Contribution

Welcome !!  
I use Docker as test environment. 
Please see `.travis.yml`
