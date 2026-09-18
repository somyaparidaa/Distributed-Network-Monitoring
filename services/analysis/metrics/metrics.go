package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	Registry = prometheus.NewRegistry()

	KafkaEventsConsumedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "analysis_kafka_events_consumed_total",
			Help: "Total number of Kafka events consumed by topic and processing result.",
		},
		[]string{"topic", "result"},
	)

	ProcessingDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "analysis_processing_duration_seconds",
			Help:    "Duration of event processing in seconds by event type.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"event_type"},
	)

	ProcessingErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "analysis_processing_errors_total",
			Help: "Total count of processing errors by event type and pipeline stage.",
		},
		[]string{"event_type", "stage"},
	)

	AnomaliesDetectedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "analysis_anomalies_detected_total",
			Help: "Total number of anomalies detected by signal and severity.",
		},
		[]string{"signal", "severity"},
	)

	StorageOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "analysis_storage_operations_total",
			Help: "Total count of storage operations by operation name and result.",
		},
		[]string{"operation", "result"},
	)

	DependencyAvailable = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "analysis_dependency_available",
			Help: "Availability status of external dependencies (1 for up, 0 for down).",
		},
		[]string{"dependency"},
	)
)

func init() {
	Registry.MustRegister(
		KafkaEventsConsumedTotal,
		ProcessingDurationSeconds,
		ProcessingErrorsTotal,
		AnomaliesDetectedTotal,
		StorageOperationsTotal,
		DependencyAvailable,
	)
}
