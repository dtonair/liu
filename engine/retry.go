package engine

import (
	"hash/fnv"
	"math"
	"time"

	"github.com/dtonair/liu/model"
)

// retryDecision is the outcome of evaluating a failed task against its policy.
type retryDecision struct {
	retry     bool
	nextVisit time.Time
}

// decideRetry determines whether a failed activity task should be retried and,
// if so, when it next becomes visible. The backoff is exponential with a cap
// and deterministic per-attempt jitter (spec FR7).
//
//   - retryable=false (worker reported a non-retryable error) -> no retry.
//   - errClass listed in policy.NonRetryableErrors -> no retry.
//   - attempt >= MaxAttempts -> no retry (budget exhausted).
//
// attempt is the attempt number that just failed (1-based).
func decideRetry(policy model.RetryPolicy, attempt int, retryable bool, errClass string, now time.Time) retryDecision {
	if !retryable {
		return retryDecision{retry: false}
	}
	for _, ne := range policy.NonRetryableErrors {
		if ne == errClass {
			return retryDecision{retry: false}
		}
	}
	if attempt >= policy.MaxAttempts {
		return retryDecision{retry: false}
	}
	return retryDecision{retry: true, nextVisit: now.Add(backoff(policy, attempt))}
}

// backoff computes the delay before retry attempt (attempt+1), applying
// exponential growth, a ceiling, and bounded deterministic jitter.
func backoff(policy model.RetryPolicy, attempt int) time.Duration {
	base := float64(policy.InitialInterval.Std())
	coeff := policy.BackoffCoefficient
	if coeff <= 0 {
		coeff = 2.0
	}
	// attempt is 1-based; the first retry uses the initial interval.
	d := base * math.Pow(coeff, float64(attempt-1))
	if max := float64(policy.MaxInterval.Std()); max > 0 && d > max {
		d = max
	}
	if policy.Jitter > 0 {
		// Deterministic jitter in [-jitter, +jitter] derived from the attempt,
		// so retries spread out without needing a RNG (which would be
		// non-deterministic and untestable).
		frac := jitterFraction(attempt)       // [0,1)
		delta := policy.Jitter * (2*frac - 1) // [-jitter, +jitter)
		d *= 1 + delta
	}
	if d < 0 {
		d = 0
	}
	return time.Duration(d)
}

// jitterFraction returns a stable pseudo-fraction in [0,1) for an attempt.
func jitterFraction(attempt int) float64 {
	h := fnv.New32a()
	var b [4]byte
	b[0] = byte(attempt)
	b[1] = byte(attempt >> 8)
	b[2] = byte(attempt >> 16)
	b[3] = byte(attempt >> 24)
	_, _ = h.Write(b[:])
	return float64(h.Sum32()%1000) / 1000.0
}
