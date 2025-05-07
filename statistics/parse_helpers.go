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
        return time.Parse(time.RFC3339, string(v))
    case string:
        return time.Parse(time.RFC3339, v)
    }
    return time.Time{}, fmt.Errorf("cannot parse time: %v", i)
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
