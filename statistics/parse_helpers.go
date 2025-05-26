// parse_helpers.go
package main

import (
	"fmt"
	"strconv"
	"time"
)

// parseFloat converts interface{} from SQL/JSON into a float64.
func parseFloat(i interface{}) (float64, error) {
	switch v := i.(type) {
	case float64:
		return v, nil
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	case string:
		return strconv.ParseFloat(v, 64)
	}
	return 0, fmt.Errorf("cannot parse float: %v", i)
}

// parseInt converts interface{} from SQL/JSON into an int.
func parseInt(i interface{}) (int, error) {
	switch v := i.(type) {
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case float64:
		return int(v), nil
	case []byte:
		return strconv.Atoi(string(v))
	case string:
		return strconv.Atoi(v)
	}
	return 0, fmt.Errorf("cannot parse int: %v", i)
}

// parseTime converts interface{} from SQL/JSON into a time.Time.
func parseTime(i interface{}) (time.Time, error) {
	switch v := i.(type) {
	case time.Time:
		return v, nil
	case []byte:
		// Try multiple formats
		str := string(v)
		return parseTimeString(str)
	case string:
		// Try multiple formats
		return parseTimeString(v)
	case nil:
		// Handle NULL values from database
		return time.Time{}, fmt.Errorf("nil timestamp")
	}
	return time.Time{}, fmt.Errorf("cannot parse time: %v", i)
}

// parseTimeString tries to parse a string as a timestamp using multiple formats
func parseTimeString(str string) (time.Time, error) {
	// Try RFC3339 format first (standard for JSON)
	if t, err := time.Parse(time.RFC3339, str); err == nil {
		return t, nil
	}

	// Try PostgreSQL timestamp format
	if t, err := time.Parse("2006-01-02 15:04:05", str); err == nil {
		return t, nil
	}

	// Try PostgreSQL timestamp with timezone format
	if t, err := time.Parse("2006-01-02 15:04:05-07", str); err == nil {
		return t, nil
	}

	// Try PostgreSQL timestamp with timezone format (with colon)
	if t, err := time.Parse("2006-01-02 15:04:05-07:00", str); err == nil {
		return t, nil
	}

	// Try PostgreSQL timestamp with microseconds
	if t, err := time.Parse("2006-01-02 15:04:05.999999", str); err == nil {
		return t, nil
	}

	// Try PostgreSQL timestamp with microseconds and timezone
	if t, err := time.Parse("2006-01-02 15:04:05.999999-07", str); err == nil {
		return t, nil
	}

	// Try PostgreSQL timestamp with microseconds and timezone (with colon)
	if t, err := time.Parse("2006-01-02 15:04:05.999999-07:00", str); err == nil {
		return t, nil
	}

	return time.Time{}, fmt.Errorf("could not parse time: %s", str)
}

// parseString converts interface{} from SQL/JSON into a string.
func parseString(i interface{}) (string, error) {
	switch v := i.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	}
	return "", fmt.Errorf("cannot parse string: %v", i)
}
