package opslog

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"testing"
)

func TestRedactedWriterDoesNotForwardComponentOutput(t *testing.T) {
	var output bytes.Buffer
	logger := log.New(&output, "", 0)
	writer := RedactedWriter(logger, "serve", "", "http_server")
	if _, err := writer.Write([]byte("Authorization: Bearer private-token at /srv/private")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !strings.Contains(output.String(), `"phase":"http_server"`) || strings.Contains(output.String(), "private-token") ||
		strings.Contains(output.String(), "/srv/private") {
		t.Fatalf("redacted output = %q", output.String())
	}
}

func TestErrorWritesStructuredFieldsWithoutSensitiveDetails(t *testing.T) {
	const (
		library = "01234567-89ab-4def-8123-456789abcdef"
		secret  = "Bearer private-token"
		path    = "/srv/private/filecloud/metadata.db"
	)
	var output bytes.Buffer
	Error(log.New(&output, "", 0), "serve", library, "authenticate_session",
		errors.New("open "+path+": Authorization "+secret+": unavailable"))

	var event record
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &event); err != nil {
		t.Fatalf("decode log %q: %v", output.String(), err)
	}
	if event.Command != "serve" || event.Library != library || event.Phase != "authenticate_session" ||
		event.ErrorCategory != _categoryUnavailable || event.Level != "error" || event.Time == "" {
		t.Fatalf("event = %+v", event)
	}
	for _, sensitive := range []string{secret, path, "Authorization", "private-token", "metadata.db"} {
		if strings.Contains(output.String(), sensitive) {
			t.Fatalf("log exposed %q: %q", sensitive, output.String())
		}
	}
}
