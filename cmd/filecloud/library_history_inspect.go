package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mingming-cn/filecloud/internal/object"
)

const _historyInspectTokenPurpose = "history-inspect-directory"

var _errHistoryInspectPathNotFound = errors.New("history path not found")

type historyInspectCommit struct {
	CommitID         string   `json:"CommitId"`
	Role             string   `json:"Role"`
	MainlineCommitID string   `json:"MainlineCommitId"`
	AuthorUserID     string   `json:"AuthorUserId"`
	CreatedAt        string   `json:"CreatedAt"`
	DeviceID         string   `json:"DeviceId"`
	Message          string   `json:"Message"`
	Parents          []string `json:"Parents"`
	Root             string   `json:"Root"`
}

type historyInspectPageToken struct {
	Version     int    `json:"Version"`
	Purpose     string `json:"Purpose"`
	LibraryID   string `json:"LibraryId"`
	CommitID    string `json:"CommitId"`
	Path        string `json:"Path"`
	DirectoryID string `json:"DirectoryId"`
	NextName    string `json:"NextName"`
}

type historyInspectTarget struct {
	kind       string
	id         string
	modifiedAt string
	file       object.File
	directory  object.Directory
}

type historyInspectClient struct {
	binding clientBinding
	base    *url.URL
	token   []byte
}

func runLibraryHistoryInspect(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("library history inspect", stderr)
	clientDir := flags.String("client-dir", "", "Filecloud client state directory")
	worktree := flags.String("worktree", "", "Bound worktree directory")
	commitID := flags.String("commit", "", "Complete historical CommitId")
	path := flags.String("path", "", "Protocol-relative snapshot path or .")
	pageSize := flags.Int("page-size", 100, "Directory entries per page")
	pageToken := flags.String("page-token", "", "Opaque next-page token")
	if err := flags.Parse(args); err != nil {
		return err
	}
	const usage = "usage: filecloud library history inspect --client-dir path --worktree path --commit 64-hex [--path relative-path-or-dot] [--page-size n] [--page-token token]"
	if *clientDir == "" || *worktree == "" || *commitID == "" || flags.NArg() != 0 {
		return errors.New(usage)
	}
	var pathSet, pageSizeSet, pageTokenSet bool
	flags.Visit(func(flag *flag.Flag) {
		switch flag.Name {
		case "path":
			pathSet = true
		case "page-size":
			pageSizeSet = true
		case "page-token":
			pageTokenSet = true
		}
	})
	if !object.ValidID(*commitID) {
		return errors.New("commit must be a complete 64-character lowercase object ID")
	}
	if pathSet && !object.ValidPath(*path) {
		return errors.New("path must be a canonical protocol-relative path or '.'")
	}
	if !pathSet && (pageSizeSet || pageTokenSet) {
		return errors.New("directory pagination requires --path")
	}
	if *pageSize < 1 || *pageSize > _maximumLibraryPageSize {
		return fmt.Errorf("page-size must be between 1 and %d", _maximumLibraryPageSize)
	}
	if len(*pageToken) > _maximumHistoryPageTokenSize {
		return errors.New("page-token is too long")
	}

	client, err := loadHistoryInspectClient(ctx, *clientDir, *worktree)
	if err != nil {
		return err
	}
	defer clear(client.token)

	var cursor historyInspectPageToken
	if *pageToken != "" {
		cursor, err = decodeHistoryInspectPageToken(*pageToken, client.token, client.binding.LibraryID, *commitID, *path)
		if err != nil {
			return errors.New("invalid history inspect page token")
		}
	}
	commit, err := client.fetchCommit(ctx, *commitID)
	if err != nil {
		return err
	}

	var output bytes.Buffer
	if err := writeHistoryInspectCommit(&output, commit); err != nil {
		return err
	}
	if !pathSet {
		_, err = io.Copy(stdout, &output)
		if err != nil {
			return fmt.Errorf("write history commit: %w", err)
		}
		return nil
	}

	target, err := client.resolvePath(ctx, commit.Root, *path)
	if errors.Is(err, _errHistoryInspectPathNotFound) {
		return errors.New("history path not found")
	}
	if err != nil {
		return err
	}
	switch target.kind {
	case "File":
		if pageSizeSet || pageTokenSet {
			return errors.New("directory pagination requires a directory path")
		}
		err = writeHistoryInspectFile(&output, *path, target)
	case "Directory":
		err = writeHistoryInspectDirectory(&output, *path, target, cursor, *pageSize, client.token, client.binding.LibraryID, *commitID)
	default:
		err = errors.New("historical snapshot contains an invalid entry type")
	}
	if err != nil {
		return err
	}
	if _, err := io.Copy(stdout, &output); err != nil {
		return fmt.Errorf("write history inspection: %w", err)
	}
	return nil
}

func loadHistoryInspectClient(ctx context.Context, clientDir, worktree string) (client *historyInspectClient, retErr error) {
	canonicalClientDir, err := canonicalStateDir(clientDir)
	if err != nil {
		return nil, err
	}
	canonicalWorktree, err := historyWorktreePath(worktree)
	if err != nil {
		return nil, err
	}
	db, err := openClientDB(filepath.Join(canonicalClientDir, _clientDatabaseName), true)
	if err != nil {
		return nil, err
	}
	var token []byte
	defer func() {
		retErr = errors.Join(retErr, db.Close())
		if retErr != nil {
			if client != nil {
				clear(client.token)
			} else {
				clear(token)
			}
		}
	}()
	var binding clientBinding
	if err := db.QueryRowContext(ctx, `SELECT server_url, library_id, worktree, user_id, device_id, access_token
		FROM bindings WHERE worktree = ?`, canonicalWorktree).Scan(
		&binding.ServerURL, &binding.LibraryID, &binding.Worktree, &binding.UserID, &binding.DeviceID, &token); err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New("worktree is not bound")
		}
		return nil, fmt.Errorf("read client binding: %w", err)
	}
	if !validClientUUID(binding.LibraryID) || !validClientUUID(binding.UserID) || binding.Worktree != canonicalWorktree {
		return nil, errors.New("client binding is invalid")
	}
	base, err := validateServerURL(binding.ServerURL)
	if err != nil {
		return nil, err
	}
	client = &historyInspectClient{binding: binding, base: base, token: token}
	return client, nil
}

func (c *historyInspectClient) fetchCommit(ctx context.Context, commitID string) (historyInspectCommit, error) {
	target := c.base.JoinPath("v1/libraries", c.binding.LibraryID, "history", commitID)
	status, data, headers, err := historyGETWithRetry(ctx, target.String(), c.token, "inspect library history")
	if err != nil {
		return historyInspectCommit{}, err
	}
	if status != http.StatusOK {
		return historyInspectCommit{}, fmt.Errorf("inspect library history failed: server returned %s", http.StatusText(status))
	}
	var envelope struct {
		RetCode       *int                  `json:"RetCode"`
		Message       *string               `json:"Message"`
		HistoryCommit *historyInspectCommit `json:"HistoryCommit"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&envelope); err != nil {
		return historyInspectCommit{}, errors.New("invalid history commit response")
	}
	if trailing, err := decoder.Token(); !errors.Is(err, io.EOF) || trailing != nil {
		return historyInspectCommit{}, errors.New("invalid history commit response")
	}
	if envelope.RetCode == nil || *envelope.RetCode != 0 || envelope.Message == nil || *envelope.Message != "success" || envelope.HistoryCommit == nil {
		return historyInspectCommit{}, errors.New("invalid history commit response")
	}
	commit := *envelope.HistoryCommit
	if commit.CommitID != commitID || commit.AuthorUserID != c.binding.UserID || !validHistoryTimestamp(commit.CreatedAt) ||
		!validClientUUID(commit.AuthorUserID) || !validClientUUID(commit.DeviceID) || !object.ValidID(commit.Root) ||
		commit.Parents == nil || len(commit.Parents) > 2 ||
		!object.ValidID(commit.MainlineCommitID) ||
		(commit.Role == "mainline" && commit.MainlineCommitID != commitID) ||
		(commit.Role == "merge-source" && commit.MainlineCommitID == commitID) ||
		(commit.Role != "mainline" && commit.Role != "merge-source") {
		return historyInspectCommit{}, errors.New("invalid history commit response")
	}
	for _, parent := range commit.Parents {
		if !object.ValidID(parent) {
			return historyInspectCommit{}, errors.New("invalid history commit response")
		}
	}
	if headers.Get("ETag") != `"`+commitID+`"` || headers.Get("Cache-Control") != "private, immutable" {
		return historyInspectCommit{}, errors.New("invalid history commit response headers")
	}
	return commit, nil
}

func (c *historyInspectClient) resolvePath(ctx context.Context, rootID, path string) (historyInspectTarget, error) {
	if path == "." {
		directory, err := c.fetchDirectory(ctx, rootID)
		return historyInspectTarget{kind: "Directory", id: rootID, directory: directory}, err
	}
	currentID := rootID
	components := strings.Split(path, "/")
	for index, component := range components {
		directory, err := c.fetchDirectory(ctx, currentID)
		if err != nil {
			return historyInspectTarget{}, err
		}
		entryIndex := sort.Search(len(directory.Entries), func(i int) bool {
			return directory.Entries[i].Name >= component
		})
		if entryIndex == len(directory.Entries) || directory.Entries[entryIndex].Name != component {
			return historyInspectTarget{}, _errHistoryInspectPathNotFound
		}
		entry := directory.Entries[entryIndex]
		if index+1 < len(components) {
			if entry.Type != "Directory" {
				return historyInspectTarget{}, _errHistoryInspectPathNotFound
			}
			currentID = entry.ID
			continue
		}
		target := historyInspectTarget{kind: entry.Type, id: entry.ID, modifiedAt: entry.ModifiedAt}
		switch entry.Type {
		case "File":
			target.file, err = c.fetchFile(ctx, entry.ID)
		case "Directory":
			target.directory, err = c.fetchDirectory(ctx, entry.ID)
		default:
			err = errors.New("historical snapshot contains an invalid entry type")
		}
		return target, err
	}
	return historyInspectTarget{}, _errHistoryInspectPathNotFound
}

func (c *historyInspectClient) fetchDirectory(ctx context.Context, id string) (object.Directory, error) {
	data, err := c.fetchObject(ctx, "directories", id)
	if err != nil {
		return object.Directory{}, err
	}
	directory, err := object.VerifyDirectory(data, id)
	if err != nil {
		return object.Directory{}, errors.New("historical directory is not valid canonical content")
	}
	return directory, nil
}

func (c *historyInspectClient) fetchFile(ctx context.Context, id string) (object.File, error) {
	data, err := c.fetchObject(ctx, "files", id)
	if err != nil {
		return object.File{}, err
	}
	file, err := object.VerifyFile(data, id)
	if err != nil {
		return object.File{}, errors.New("historical file is not valid canonical content")
	}
	return file, nil
}

func (c *historyInspectClient) fetchObject(ctx context.Context, kind, id string) ([]byte, error) {
	if !object.ValidID(id) || (kind != "directories" && kind != "files") {
		return nil, errors.New("historical snapshot contains an invalid object reference")
	}
	target := c.base.JoinPath("v1/libraries", c.binding.LibraryID, "objects", kind, id)
	status, data, _, err := historyGETWithRetry(ctx, target.String(), c.token, "read history metadata")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, errors.New("historical snapshot metadata is unavailable")
	}
	return data, nil
}

func writeHistoryInspectCommit(output io.Writer, commit historyInspectCommit) error {
	_, err := fmt.Fprintf(output, "CommitId=%s\nRole=%s\nMainlineCommitId=%s\nAuthorUserId=%s\nCreatedAt=%s\nDeviceId=%s\nMessage=%q\nParents=%s\nRoot=%s\n",
		commit.CommitID, commit.Role, commit.MainlineCommitID, commit.AuthorUserID, commit.CreatedAt, commit.DeviceID,
		commit.Message, strings.Join(commit.Parents, ","), commit.Root)
	if err != nil {
		return fmt.Errorf("format history commit: %w", err)
	}
	return nil
}

func writeHistoryInspectFile(output io.Writer, path string, target historyInspectTarget) error {
	_, err := fmt.Fprintf(output, "Path=%s\nType=File\nFileId=%s\nSize=%d\nModifiedAt=%s\nBlocks=%d\n",
		path, target.id, target.file.Size, target.modifiedAt, len(target.file.Blocks))
	if err != nil {
		return fmt.Errorf("format historical file: %w", err)
	}
	return nil
}

func writeHistoryInspectDirectory(output io.Writer, path string, target historyInspectTarget, cursor historyInspectPageToken, pageSize int, token []byte, libraryID, commitID string) error {
	start := 0
	if cursor.NextName != "" {
		if cursor.DirectoryID != target.id {
			return errors.New("invalid history inspect page token")
		}
		start = sort.Search(len(target.directory.Entries), func(i int) bool {
			return target.directory.Entries[i].Name >= cursor.NextName
		})
		if start == len(target.directory.Entries) || target.directory.Entries[start].Name != cursor.NextName {
			return errors.New("invalid history inspect page token")
		}
	}
	end := min(start+pageSize, len(target.directory.Entries))
	modifiedAt := target.modifiedAt
	if modifiedAt == "" {
		modifiedAt = "-"
	}
	if _, err := fmt.Fprintf(output, "Path=%s\nType=Directory\nDirectoryId=%s\nModifiedAt=%s\n", path, target.id, modifiedAt); err != nil {
		return fmt.Errorf("format historical directory: %w", err)
	}
	for _, entry := range target.directory.Entries[start:end] {
		if _, err := fmt.Fprintf(output, "Entry name=%s type=%s id=%s modified_at=%s\n", entry.Name, entry.Type, entry.ID, entry.ModifiedAt); err != nil {
			return fmt.Errorf("format historical directory entry: %w", err)
		}
	}
	if end < len(target.directory.Entries) {
		next, err := encodeHistoryInspectPageToken(token, historyInspectPageToken{
			Version: 1, Purpose: _historyInspectTokenPurpose, LibraryID: libraryID, CommitID: commitID, Path: path,
			DirectoryID: target.id, NextName: target.directory.Entries[end].Name,
		})
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(output, "next_page_token=%s\n", next); err != nil {
			return fmt.Errorf("format historical directory cursor: %w", err)
		}
	}
	return nil
}

func encodeHistoryInspectPageToken(token []byte, value historyInspectPageToken) (string, error) {
	aead, err := historyInspectTokenAEAD(token)
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode history inspect page token: %w", err)
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("create history inspect page token: %w", err)
	}
	sealed := aead.Seal(nonce, nonce, payload, []byte(_historyInspectTokenPurpose))
	encoded := base64.RawURLEncoding.EncodeToString(sealed)
	if len(encoded) > _maximumHistoryPageTokenSize {
		return "", errors.New("history inspect page token exceeds size limit")
	}
	return encoded, nil
}

func decodeHistoryInspectPageToken(encoded string, token []byte, libraryID, commitID, path string) (historyInspectPageToken, error) {
	if len(encoded) == 0 || len(encoded) > _maximumHistoryPageTokenSize {
		return historyInspectPageToken{}, errors.New("invalid history inspect page token")
	}
	aead, err := historyInspectTokenAEAD(token)
	if err != nil {
		return historyInspectPageToken{}, err
	}
	sealed, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || len(sealed) < aead.NonceSize()+aead.Overhead() {
		return historyInspectPageToken{}, errors.New("invalid history inspect page token")
	}
	nonce := sealed[:aead.NonceSize()]
	payload, err := aead.Open(nil, nonce, sealed[aead.NonceSize():], []byte(_historyInspectTokenPurpose))
	if err != nil {
		return historyInspectPageToken{}, errors.New("invalid history inspect page token")
	}
	var value historyInspectPageToken
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return historyInspectPageToken{}, errors.New("invalid history inspect page token")
	}
	if trailing, err := decoder.Token(); !errors.Is(err, io.EOF) || trailing != nil {
		return historyInspectPageToken{}, errors.New("invalid history inspect page token")
	}
	if value.Version != 1 || value.Purpose != _historyInspectTokenPurpose || value.LibraryID != libraryID || value.CommitID != commitID || value.Path != path ||
		!object.ValidID(value.DirectoryID) || !object.ValidPath(value.NextName) || value.NextName == "." || strings.Contains(value.NextName, "/") {
		return historyInspectPageToken{}, errors.New("invalid history inspect page token")
	}
	return value, nil
}

func historyInspectTokenAEAD(token []byte) (cipher.AEAD, error) {
	material := make([]byte, len(_historyInspectTokenPurpose)+1+len(token))
	copy(material, _historyInspectTokenPurpose)
	copy(material[len(_historyInspectTokenPurpose)+1:], token)
	key := sha256.Sum256(material)
	clear(material)
	defer clear(key[:])
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fmt.Errorf("create history inspect page token cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create history inspect page token aead: %w", err)
	}
	return aead, nil
}
