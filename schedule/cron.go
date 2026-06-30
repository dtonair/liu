// Package schedule contains cron helpers for workflow schedules.
package schedule

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	minuteIndex = iota
	hourIndex
	dayOfMonthIndex
	monthIndex
	dayOfWeekIndex
)

var fieldRanges = []struct {
	name string
	min  int
	max  int
}{
	{name: "minute", min: 0, max: 59},
	{name: "hour", min: 0, max: 23},
	{name: "day-of-month", min: 1, max: 31},
	{name: "month", min: 1, max: 12},
	{name: "day-of-week", min: 0, max: 7},
}

// Cron is a parsed standard 5-field cron expression.
type Cron struct {
	expr   string
	fields [5]cronField
}

type cronField struct {
	allowed [60]bool
	any     bool
}

// ParseCron parses a standard 5-field cron expression:
// minute hour day-of-month month day-of-week.
//
// Supported field syntax is *, numeric values, lists, ranges, and steps. Month
// and weekday names are intentionally not supported in v1.
func ParseCron(expr string) (*Cron, error) {
	parts := strings.Fields(expr)
	if len(parts) != 5 {
		return nil, fmt.Errorf("cron expression must have 5 fields, got %d", len(parts))
	}
	c := &Cron{expr: expr}
	for i, part := range parts {
		field, err := parseField(part, fieldRanges[i].name, fieldRanges[i].min, fieldRanges[i].max)
		if err != nil {
			return nil, err
		}
		c.fields[i] = field
	}
	return c, nil
}

// MustParseCron parses expr and panics on error. It is intended for tests.
func MustParseCron(expr string) *Cron {
	c, err := ParseCron(expr)
	if err != nil {
		panic(err)
	}
	return c
}

// Expr returns the original expression.
func (c *Cron) Expr() string { return c.expr }

// NextAfter returns the next matching time strictly after after, interpreting
// the cron expression in loc and returning the result in UTC.
func (c *Cron) NextAfter(after time.Time, loc *time.Location) (time.Time, error) {
	if loc == nil {
		return time.Time{}, fmt.Errorf("location is required")
	}
	local := after.In(loc).Truncate(time.Minute).Add(time.Minute)
	deadline := local.AddDate(5, 0, 0)
	for !local.After(deadline) {
		if c.matches(local) {
			return local.UTC(), nil
		}
		local = local.Add(time.Minute)
	}
	return time.Time{}, fmt.Errorf("no cron occurrence found within 5 years")
}

// NextAfterInLocation validates timezone, then returns the next matching UTC
// time strictly after after.
func NextAfterInLocation(expr, timezone string, after time.Time) (time.Time, error) {
	if timezone == "" {
		timezone = "UTC"
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid timezone %q: %w", timezone, err)
	}
	c, err := ParseCron(expr)
	if err != nil {
		return time.Time{}, err
	}
	return c.NextAfter(after, loc)
}

func (c *Cron) matches(t time.Time) bool {
	month := int(t.Month())
	day := t.Day()
	weekday := int(t.Weekday())
	dom := c.fields[dayOfMonthIndex]
	dow := c.fields[dayOfWeekIndex]
	domMatch := dom.allowed[day]
	dowMatch := dow.allowed[weekday] || (weekday == 0 && dow.allowed[7])
	dayMatch := domMatch && dowMatch
	if !dom.any && !dow.any {
		// Standard cron semantics: when both day fields are restricted, either
		// may match.
		dayMatch = domMatch || dowMatch
	}
	return c.fields[minuteIndex].allowed[t.Minute()] &&
		c.fields[hourIndex].allowed[t.Hour()] &&
		c.fields[monthIndex].allowed[month] &&
		dayMatch
}

func parseField(expr, name string, minVal, maxVal int) (cronField, error) {
	var field cronField
	if expr == "" {
		return field, fmt.Errorf("cron %s field is empty", name)
	}
	if expr == "*" {
		field.any = true
	}
	for _, item := range strings.Split(expr, ",") {
		if item == "" {
			return field, fmt.Errorf("cron %s field contains an empty list item", name)
		}
		start, end, step, any, err := parseFieldItem(item, name, minVal, maxVal)
		if err != nil {
			return field, err
		}
		if any {
			field.any = true
		}
		for v := start; v <= end; v += step {
			field.allowed[v] = true
			if name == "day-of-week" && v == 7 {
				field.allowed[0] = true
			}
		}
	}
	return field, nil
}

func parseFieldItem(item, name string, minVal, maxVal int) (start int, end int, step int, any bool, err error) {
	step = 1
	base := item
	if strings.Contains(item, "/") {
		parts := strings.Split(item, "/")
		if len(parts) != 2 || parts[1] == "" {
			return 0, 0, 0, false, fmt.Errorf("cron %s field has invalid step %q", name, item)
		}
		base = parts[0]
		step, err = strconv.Atoi(parts[1])
		if err != nil || step <= 0 {
			return 0, 0, 0, false, fmt.Errorf("cron %s field has invalid step %q", name, item)
		}
	}
	switch {
	case base == "*":
		return minVal, maxVal, step, true, nil
	case strings.Contains(base, "-"):
		parts := strings.Split(base, "-")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return 0, 0, 0, false, fmt.Errorf("cron %s field has invalid range %q", name, item)
		}
		start, err = parseNumber(parts[0], name, minVal, maxVal)
		if err != nil {
			return 0, 0, 0, false, err
		}
		end, err = parseNumber(parts[1], name, minVal, maxVal)
		if err != nil {
			return 0, 0, 0, false, err
		}
		if start > end {
			return 0, 0, 0, false, fmt.Errorf("cron %s field range %q is descending", name, item)
		}
		return start, end, step, false, nil
	default:
		start, err = parseNumber(base, name, minVal, maxVal)
		if err != nil {
			return 0, 0, 0, false, err
		}
		return start, start, step, false, nil
	}
}

func parseNumber(s, name string, minVal, maxVal int) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("cron %s field has invalid number %q", name, s)
	}
	if n < minVal || n > maxVal {
		return 0, fmt.Errorf("cron %s field value %d outside range %d-%d", name, n, minVal, maxVal)
	}
	return n, nil
}
