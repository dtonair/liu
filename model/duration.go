package model

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration is a time.Duration that (un)marshals to/from a Go duration string
// such as "24h" or "1m30s" in JSON, instead of an integer nanosecond count.
type Duration time.Duration

// MarshalJSON renders the duration as a string (e.g. "24h0m0s").
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON accepts either a duration string ("24h") or a numeric
// nanosecond count, so hand-written DSL and machine output both parse.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch val := v.(type) {
	case string:
		parsed, err := time.ParseDuration(val)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", val, err)
		}
		*d = Duration(parsed)
	case float64:
		*d = Duration(time.Duration(val))
	default:
		return fmt.Errorf("duration must be a string or number, got %T", v)
	}
	return nil
}

// Std returns the value as a standard library time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }
