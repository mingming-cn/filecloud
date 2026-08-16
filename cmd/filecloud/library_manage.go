package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"
)

const _maximumLibraryPageSize = 500

type libraryCommandLibrary struct {
	LibraryID string `json:"LibraryId"`
	Name      string
	ETag      string
}

func runLibraryCreate(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flags := newFlagSet("library create", stderr)
	server := flags.String("server", "", "Filecloud server URL")
	libraryID := flags.String("library-id", "", "Library ID")
	name := flags.String("name", "", "Library name")
	tokenStdin := flags.Bool("token-stdin", false, "Read token from standard input")
	if err := flags.Parse(args); err != nil {
		return err
	}
	const usage = "usage: filecloud library create --server url --library-id uuid --name name --token-stdin"
	if *server == "" || *libraryID == "" || *name == "" || !*tokenStdin || flags.NArg() != 0 {
		return errors.New(usage)
	}
	base, err := validateServerURL(*server)
	if err != nil {
		return err
	}
	if !validClientUUID(*libraryID) {
		return errors.New("library-id must be a canonical UUID")
	}
	token, err := readLineSecret(stdin, 4096, "token")
	if err != nil {
		return err
	}
	defer clear(token)
	body, err := json.Marshal(struct{ Name string }{Name: *name})
	if err != nil {
		return fmt.Errorf("encode library: %w", err)
	}
	target := base.JoinPath("v1/libraries", *libraryID).String()
	return writeLibraryCommandResponse(ctx, "create library", func() (*http.Request, error) {
		request, err := authenticatedRequest(ctx, http.MethodPut, target, token, body)
		if err == nil {
			request.Header.Set("Content-Type", "application/json")
		}
		return request, err
	}, func(data []byte) error {
		return validateLibraryCommandEnvelope(data, *libraryID, *name)
	}, stdout, http.StatusOK, http.StatusCreated)
}

func runLibraryList(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flags := newFlagSet("library list", stderr)
	server := flags.String("server", "", "Filecloud server URL")
	pageSize := flags.Int("page-size", 100, "Libraries per page")
	pageToken := flags.String("page-token", "", "Opaque next-page token")
	tokenStdin := flags.Bool("token-stdin", false, "Read token from standard input")
	if err := flags.Parse(args); err != nil {
		return err
	}
	const usage = "usage: filecloud library list --server url [--page-size n] [--page-token token] --token-stdin"
	if *server == "" || !*tokenStdin || flags.NArg() != 0 {
		return errors.New(usage)
	}
	if *pageSize < 1 || *pageSize > _maximumLibraryPageSize {
		return fmt.Errorf("page-size must be between 1 and %d", _maximumLibraryPageSize)
	}
	base, err := validateServerURL(*server)
	if err != nil {
		return err
	}
	token, err := readLineSecret(stdin, 4096, "token")
	if err != nil {
		return err
	}
	defer clear(token)
	query := url.Values{"PageSize": {strconv.Itoa(*pageSize)}}
	if *pageToken != "" {
		query.Set("PageToken", *pageToken)
	}
	target := base.JoinPath("v1/libraries")
	target.RawQuery = query.Encode()
	return writeLibraryCommandResponse(ctx, "list libraries", func() (*http.Request, error) {
		return authenticatedRequest(ctx, http.MethodGet, target.String(), token, nil)
	}, validateLibraryListEnvelope, stdout, http.StatusOK)
}

func runLibraryInspect(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flags := newFlagSet("library inspect", stderr)
	server := flags.String("server", "", "Filecloud server URL")
	libraryID := flags.String("library-id", "", "Library ID")
	tokenStdin := flags.Bool("token-stdin", false, "Read token from standard input")
	if err := flags.Parse(args); err != nil {
		return err
	}
	const usage = "usage: filecloud library inspect --server url --library-id uuid --token-stdin"
	if *server == "" || *libraryID == "" || !*tokenStdin || flags.NArg() != 0 {
		return errors.New(usage)
	}
	base, err := validateServerURL(*server)
	if err != nil {
		return err
	}
	if !validClientUUID(*libraryID) {
		return errors.New("library-id must be a canonical UUID")
	}
	token, err := readLineSecret(stdin, 4096, "token")
	if err != nil {
		return err
	}
	defer clear(token)
	target := base.JoinPath("v1/libraries", *libraryID).String()
	return writeLibraryCommandResponse(ctx, "inspect library", func() (*http.Request, error) {
		return authenticatedRequest(ctx, http.MethodGet, target, token, nil)
	}, func(data []byte) error {
		return validateLibraryCommandEnvelope(data, *libraryID, "")
	}, stdout, http.StatusOK)
}

func writeLibraryCommandResponse(ctx context.Context, operation string, request func() (*http.Request, error),
	validate func([]byte) error, stdout io.Writer, allowedStatuses ...int,
) error {
	for attempt := 0; attempt < _headUpdateAttempts; attempt++ {
		current, err := request()
		if err != nil {
			return err
		}
		status, data, headers, err := doClientRequestWithHeaders(current)
		if err != nil {
			if attempt+1 == _headUpdateAttempts {
				return fmt.Errorf("%s after %d attempts: %w", operation, _headUpdateAttempts, err)
			}
			if err := waitTransientRetry(ctx, "", time.Now()); err != nil {
				return fmt.Errorf("wait to retry %s: %w", operation, err)
			}
			continue
		}
		if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable {
			if attempt+1 == _headUpdateAttempts {
				return fmt.Errorf("%s failed after %d attempts: server returned %s", operation, _headUpdateAttempts, http.StatusText(status))
			}
			if err := waitTransientRetry(ctx, headers.Get("Retry-After"), time.Now()); err != nil {
				return fmt.Errorf("wait to retry %s: %w", operation, err)
			}
			continue
		}
		if !slices.Contains(allowedStatuses, status) {
			return fmt.Errorf("%s failed: server returned %s", operation, http.StatusText(status))
		}
		if err := validate(data); err != nil {
			return fmt.Errorf("invalid %s response: %w", operation, err)
		}
		if _, err := io.Copy(stdout, bytes.NewReader(data)); err != nil {
			return fmt.Errorf("write %s response: %w", operation, err)
		}
		return nil
	}
	return fmt.Errorf("%s exhausted retry attempts", operation)
}

func validateLibraryCommandEnvelope(data []byte, expectedID, expectedName string) error {
	var envelope struct {
		RetCode *int
		Message *string
		Library *libraryCommandLibrary
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	if envelope.RetCode == nil || *envelope.RetCode != 0 || envelope.Message == nil || *envelope.Message != "success" || envelope.Library == nil {
		return errors.New("missing success envelope")
	}
	if envelope.Library.LibraryID != expectedID || envelope.Library.Name == "" || envelope.Library.ETag == "" ||
		(expectedName != "" && envelope.Library.Name != expectedName) {
		return errors.New("invalid library payload")
	}
	return nil
}

func validateLibraryListEnvelope(data []byte) error {
	var envelope struct {
		RetCode       *int
		Message       *string
		Libraries     *[]libraryCommandLibrary
		NextPageToken *string
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	if envelope.RetCode == nil || *envelope.RetCode != 0 || envelope.Message == nil || *envelope.Message != "success" ||
		envelope.Libraries == nil || envelope.NextPageToken == nil {
		return errors.New("missing success envelope")
	}
	for _, library := range *envelope.Libraries {
		if !validClientUUID(library.LibraryID) || library.Name == "" || library.ETag == "" {
			return errors.New("invalid library payload")
		}
	}
	return nil
}
