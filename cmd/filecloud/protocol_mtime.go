package main

import (
	"errors"
	"time"
)

const protocolMtimeLayout = "2006-01-02T15:04:05Z"

func canonicalProtocolMtime(value time.Time) string {
	return value.UTC().Truncate(time.Second).Format(protocolMtimeLayout)
}

func parseCanonicalProtocolMtime(value string) (time.Time, error) {
	parsed, err := time.Parse(protocolMtimeLayout, value)
	if err != nil || canonicalProtocolMtime(parsed) != value {
		return time.Time{}, errors.New("mtime is not canonical protocol UTC")
	}
	return parsed, nil
}
