package telemetry

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the engine's Prometheus instruments. A nil *Metrics is safe to
// use: every method is a no-op, so instrumentation never needs nil checks at
// call sites.
type Metrics struct {
	reg *prometheus.Registry

	transitions       *prometheus.CounterVec // by event type
	tasksCompleted    prometheus.Counter
	tasksFailed       prometheus.Counter
	retries           prometheus.Counter
	timersFired       prometheus.Counter
	leasesReclaimed   prometheus.Counter
	advanceLatency    prometheus.Histogram
	instancesByStatus *prometheus.GaugeVec
}

// NewMetrics registers the engine instruments on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "liu_transitions_total", Help: "Workflow state transitions by event type.",
		}, []string{"event"}),
		tasksCompleted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "liu_tasks_completed_total", Help: "Tasks completed successfully.",
		}),
		tasksFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "liu_tasks_failed_total", Help: "Tasks that failed terminally.",
		}),
		retries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "liu_task_retries_total", Help: "Task retries scheduled.",
		}),
		timersFired: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "liu_timers_fired_total", Help: "Durable timers fired.",
		}),
		leasesReclaimed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "liu_leases_reclaimed_total", Help: "Expired task leases reclaimed.",
		}),
		advanceLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "liu_advance_seconds", Help: "Latency of an instance advance transaction.",
			Buckets: prometheus.DefBuckets,
		}),
		instancesByStatus: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "liu_instances", Help: "Instance count by status (sampled).",
		}, []string{"status"}),
	}
	reg.MustRegister(m.transitions, m.tasksCompleted, m.tasksFailed, m.retries,
		m.timersFired, m.leasesReclaimed, m.advanceLatency, m.instancesByStatus)
	return m
}

// Handler returns the Prometheus scrape handler for /metrics.
func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Transition records a state transition of the given event type.
func (m *Metrics) Transition(event string) {
	if m != nil {
		m.transitions.WithLabelValues(event).Inc()
	}
}

// TaskCompleted records a successful task.
func (m *Metrics) TaskCompleted() {
	if m != nil {
		m.tasksCompleted.Inc()
	}
}

// TaskFailed records a terminal task failure.
func (m *Metrics) TaskFailed() {
	if m != nil {
		m.tasksFailed.Inc()
	}
}

// RetryScheduled records a scheduled retry.
func (m *Metrics) RetryScheduled() {
	if m != nil {
		m.retries.Inc()
	}
}

// TimerFired records a fired timer.
func (m *Metrics) TimerFired() {
	if m != nil {
		m.timersFired.Inc()
	}
}

// LeasesReclaimed records n reclaimed leases.
func (m *Metrics) LeasesReclaimed(n int) {
	if m != nil && n > 0 {
		m.leasesReclaimed.Add(float64(n))
	}
}

// ObserveAdvance records the duration of an advance transaction in seconds.
func (m *Metrics) ObserveAdvance(seconds float64) {
	if m != nil {
		m.advanceLatency.Observe(seconds)
	}
}

// SetInstanceCount sets the sampled gauge for a status.
func (m *Metrics) SetInstanceCount(status string, n int) {
	if m != nil {
		m.instancesByStatus.WithLabelValues(status).Set(float64(n))
	}
}
