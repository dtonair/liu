package schedule

import (
	"testing"
	"time"
)

func TestParseCronValid(t *testing.T) {
	valid := []string{
		"* * * * *",
		"*/5 * * * *",
		"0 9 * * 1",
		"15,45 8-17/2 1,15 * 0",
		"0 0 1 1 *",
		"0 0 * * 7",
	}
	for _, expr := range valid {
		t.Run(expr, func(t *testing.T) {
			if _, err := ParseCron(expr); err != nil {
				t.Fatalf("ParseCron(%q): %v", expr, err)
			}
		})
	}
}

func TestParseCronInvalid(t *testing.T) {
	invalid := []string{
		"",
		"* * * *",
		"* * * * * *",
		"60 * * * *",
		"* 24 * * *",
		"* * 0 * *",
		"* * * 13 *",
		"* * * * 8",
		"*/0 * * * *",
		"10-5 * * * *",
		"MON * * * *",
		"1,,2 * * * *",
	}
	for _, expr := range invalid {
		t.Run(expr, func(t *testing.T) {
			if _, err := ParseCron(expr); err == nil {
				t.Fatalf("ParseCron(%q) unexpectedly succeeded", expr)
			}
		})
	}
}

func TestNextAfterMinutePrecision(t *testing.T) {
	loc := time.UTC
	base := time.Date(2026, 6, 30, 12, 34, 56, 789, loc)
	got, err := MustParseCron("*/15 * * * *").NextAfter(base, loc)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 6, 30, 12, 45, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestNextAfterDailyTimezone(t *testing.T) {
	after := time.Date(2026, 6, 30, 1, 0, 0, 0, time.UTC)
	got, err := NextAfterInLocation("0 9 * * *", "Asia/Ho_Chi_Minh", after)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 6, 30, 2, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestNextAfterWeekdaySundaySeven(t *testing.T) {
	loc := time.UTC
	after := time.Date(2026, 6, 30, 0, 0, 0, 0, loc) // Tuesday.
	got, err := MustParseCron("0 9 * * 7").NextAfter(after, loc)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 7, 5, 9, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestNextAfterDayOfMonthOrDayOfWeek(t *testing.T) {
	loc := time.UTC
	after := time.Date(2026, 6, 30, 0, 0, 0, 0, loc) // Tuesday.
	got, err := MustParseCron("0 9 15 * 1").NextAfter(after, loc)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 7, 6, 9, 0, 0, 0, time.UTC) // Monday before the 15th.
	if !got.Equal(want) {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestNextAfterInvalidTimezone(t *testing.T) {
	_, err := NextAfterInLocation("0 9 * * *", "No/SuchZone", time.Now())
	if err == nil {
		t.Fatal("expected invalid timezone error")
	}
}

func TestNextAfterRequiresLocation(t *testing.T) {
	_, err := MustParseCron("* * * * *").NextAfter(time.Now(), nil)
	if err == nil {
		t.Fatal("expected location error")
	}
}
