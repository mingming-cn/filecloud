package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/mingming-cn/filecloud/internal/object"
)

const _maximumHistoryPageTokenSize = 4096

type historyCommandCommit struct {
	CommitID     string   `json:"CommitId"`
	AuthorUserID string   `json:"AuthorUserId"`
	CreatedAt    string   `json:"CreatedAt"`
	DeviceID     string   `json:"DeviceId"`
	Message      string   `json:"Message"`
	Parents      []string `json:"Parents"`
	Root         string   `json:"Root"`
}

type historyCommandResponse struct {
	AnchorCommitID *string                `json:"AnchorCommitId"`
	Commits        []historyCommandCommit `json:"Commits"`
	NextPageToken  string                 `json:"NextPageToken"`
}

func runLibraryHistoryList(ctx context.Context, args []string, stdout, stderr io.Writer) (retErr error) {
	flags := newFlagSet("library history list", stderr)
	clientDir := flags.String("client-dir", "", "Filecloud client state directory")
	worktree := flags.String("worktree", "", "Bound worktree directory")
	pageSize := flags.Int("page-size", 100, "Commits per page")
	pageToken := flags.String("page-token", "", "Opaque next-page token")
	if err := flags.Parse(args); err != nil {
		return err
	}
	const usage = "usage: filecloud library history list --client-dir path --worktree path [--page-size n] [--page-token token]"
	if *clientDir == "" || *worktree == "" || flags.NArg() != 0 {
		return errors.New(usage)
	}
	if *pageSize < 1 || *pageSize > _maximumLibraryPageSize {
		return fmt.Errorf("page-size must be between 1 and %d", _maximumLibraryPageSize)
	}
	if len(*pageToken) > _maximumHistoryPageTokenSize {
		return errors.New("page-token is too long")
	}

	canonicalClientDir, err := canonicalStateDir(*clientDir)
	if err != nil {
		return err
	}
	canonicalWorktree, err := historyWorktreePath(*worktree)
	if err != nil {
		return err
	}
	databasePath := filepath.Join(canonicalClientDir, _clientDatabaseName)
	db, err := openClientDB(databasePath, true)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()

	var binding clientBinding
	var token []byte
	if err := db.QueryRowContext(ctx, `SELECT server_url, library_id, worktree, user_id, device_id, access_token
		FROM bindings WHERE worktree = ?`, canonicalWorktree).Scan(
		&binding.ServerURL, &binding.LibraryID, &binding.Worktree, &binding.UserID, &binding.DeviceID, &token); err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, sql.ErrNoRows) {
			return errors.New("worktree is not bound")
		}
		return fmt.Errorf("read client binding: %w", err)
	}
	defer clear(token)
	if !validClientUUID(binding.LibraryID) || !validClientUUID(binding.UserID) || binding.Worktree != canonicalWorktree {
		return errors.New("client binding is invalid")
	}
	base, err := validateServerURL(binding.ServerURL)
	if err != nil {
		return err
	}
	query := url.Values{"PageSize": {strconv.Itoa(*pageSize)}}
	if *pageToken != "" {
		query.Set("PageToken", *pageToken)
	}
	target := base.JoinPath("v1/libraries", binding.LibraryID, "history")
	target.RawQuery = query.Encode()
	var status int
	var data []byte
	var headers http.Header
	var requestErr error
	for attempt := 0; attempt < _headUpdateAttempts; attempt++ {
		var request *http.Request
		request, requestErr = authenticatedRequest(ctx, http.MethodGet, target.String(), token, nil)
		if requestErr != nil {
			return requestErr
		}
		status, data, headers, requestErr = doClientRequestWithHeaders(request)
		if requestErr != nil {
			if attempt+1 == _headUpdateAttempts {
				return fmt.Errorf("list library history unavailable after %d attempts", _headUpdateAttempts)
			}
			if err := waitTransientRetry(ctx, "", time.Now()); err != nil {
				return fmt.Errorf("wait to retry library history: %w", err)
			}
			continue
		}
		if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable {
			if attempt+1 == _headUpdateAttempts {
				return fmt.Errorf("list library history failed after %d attempts: server returned %s", _headUpdateAttempts, http.StatusText(status))
			}
			if err := waitTransientRetry(ctx, headers.Get("Retry-After"), time.Now()); err != nil {
				return fmt.Errorf("wait to retry library history: %w", err)
			}
			continue
		}
		break
	}
	if status != http.StatusOK {
		return fmt.Errorf("list library history failed: server returned %s", http.StatusText(status))
	}
	response, err := decodeHistoryCommandResponse(data)
	if err != nil {
		return fmt.Errorf("invalid list library history response: %w", err)
	}
	for _, commit := range response.Commits {
		if _, err := fmt.Fprintf(stdout, "%s author=%s created_at=%s device=%s message=%q parents=%d root=%s\n",
			commit.CommitID, commit.AuthorUserID, commit.CreatedAt, commit.DeviceID[:8], commit.Message, len(commit.Parents), commit.Root); err != nil {
			return fmt.Errorf("write library history: %w", err)
		}
	}
	if response.NextPageToken != "" {
		if _, err := fmt.Fprintf(stdout, "next_page_token=%s\n", response.NextPageToken); err != nil {
			return fmt.Errorf("write library history cursor: %w", err)
		}
	}
	return nil
}

func historyWorktreePath(path string) (string, error) {
	return canonicalExistingPath(path)
}

func decodeHistoryCommandResponse(data []byte) (historyCommandResponse, error) {
	var envelope struct {
		RetCode *int                    `json:"RetCode"`
		Message *string                 `json:"Message"`
		History *historyCommandResponse `json:"History"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&envelope); err != nil {
		return historyCommandResponse{}, err
	}
	if trailing, err := decoder.Token(); !errors.Is(err, io.EOF) || trailing != nil {
		return historyCommandResponse{}, errors.New("trailing history response data")
	}
	if envelope.RetCode == nil || *envelope.RetCode != 0 || envelope.Message == nil || *envelope.Message != "success" || envelope.History == nil {
		return historyCommandResponse{}, errors.New("missing success envelope")
	}
	response := *envelope.History
	if response.Commits == nil || len(response.NextPageToken) > _maximumHistoryPageTokenSize {
		return historyCommandResponse{}, errors.New("invalid history page")
	}
	if response.AnchorCommitID != nil && !object.ValidID(*response.AnchorCommitID) {
		return historyCommandResponse{}, errors.New("invalid history anchor")
	}
	for _, commit := range response.Commits {
		if !object.ValidID(commit.CommitID) || !validClientUUID(commit.AuthorUserID) || !validClientUUID(commit.DeviceID) ||
			!validHistoryTimestamp(commit.CreatedAt) || !object.ValidID(commit.Root) || commit.Parents == nil {
			return historyCommandResponse{}, errors.New("invalid history commit")
		}
		for _, parent := range commit.Parents {
			if !object.ValidID(parent) {
				return historyCommandResponse{}, errors.New("invalid history parent")
			}
		}
	}
	return response, nil
}

func validHistoryTimestamp(value string) bool {
	parsed, err := time.Parse("2006-01-02T15:04:05Z", value)
	return err == nil && parsed.UTC().Format("2006-01-02T15:04:05Z") == value
}
