package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	Registry = prometheus.NewRegistry()

	PollAttemptsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "monitoring_poll_attempts_total",
			Help: "Total number of device polling attempts, including retries.",
		},
		[]string{"result"},
	)

	PollDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "monitoring_poll_duration_seconds",
			Help:    "Duration of device poll execution in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"result"},
	)

	DevicesMonitoredTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "monitoring_devices_monitored_total",
			Help: "Total number of monitored devices by tracking status.",
		},
		[]string{"status"},
	)

	HealthEvaluationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "monitoring_health_evaluations_total",
			Help: "Total count of health evaluations completed by status outcome.",
		},
		[]string{"status"},
	)

	HealthTransitionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "monitoring_health_transitions_total",
			Help: "Total count of device health status transitions.",
		},
		[]string{"from", "to"},
	)

	KafkaPublishesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "monitoring_kafka_publishes_total",
			Help: "Total number of Kafka message publishing attempts.",
		},
		[]string{"topic", "result"},
	)

	KafkaAvailable = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "monitoring_kafka_available",
			Help: "Indicates whether the Kafka broker/producer is operational (1 for up, 0 for down).",
		},
	)
)

func init() {
	Registry.MustRegister(
		PollAttemptsTotal,
		PollDurationSeconds,
		DevicesMonitoredTotal,
		HealthEvaluationsTotal,
		HealthTransitionsTotal,
		KafkaPublishesTotal,
		KafkaAvailable,
	)
}
