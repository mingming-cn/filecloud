// Package object validates and canonically encodes immutable content objects.
package object

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const (
	// MaxBlockSize is the protocol block boundary.
	MaxBlockSize    = 4 << 20
	_maxFileSize    = 1 << 40
	_maxBlocks      = 262144
	_maxEntries     = 100000
	_maxParents     = 2
	_maxJSONNesting = 256
)

var (
	// ErrPayloadTooLarge reports a metadata structure exceeding protocol budgets.
	ErrPayloadTooLarge = errors.New("object payload too large")
)

// ID returns the lowercase SHA-256 identity of canonical bytes.
func ID(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// ValidID reports whether value is a canonical ObjectId.
func ValidID(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// Canonicalize validates one metadata object and returns its JCS bytes and identity.
func Canonicalize(kind string, data []byte) ([]byte, string, error) {
	if !utf8.Valid(data) || !validEscapedSurrogates(data) {
		return nil, "", errors.New("object is not valid unicode JSON")
	}
	var canonical []byte
	var err error
	switch kind {
	case "files":
		canonical, err = canonicalFile(data)
	case "directories":
		canonical, err = canonicalDirectory(data)
	case "commits":
		canonical, err = canonicalCommit(data)
	default:
		err = errors.New("invalid metadata object type")
	}
	if err != nil {
		return nil, "", err
	}
	return canonical, ID(canonical), nil
}

type fileObject struct {
	Blocks  []string `json:"Blocks"`
	Size    string   `json:"Size"`
	Type    string   `json:"Type"`
	Version int      `json:"Version"`
}

func decodeFile(data []byte) (fileObject, error) {
	if err := checkFileBudgets(data); err != nil {
		return fileObject{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decodeJSONContainer(decoder, json.Delim('{'), 1, "file object"); err != nil {
		return fileObject{}, err
	}
	var value fileObject
	seen := make(map[string]struct{}, 4)
	for decoder.More() {
		fieldToken, err := decoder.Token()
		if err != nil {
			return fileObject{}, fmt.Errorf("decode file field: %w", err)
		}
		field, ok := fieldToken.(string)
		if !ok {
			return fileObject{}, errors.New("file field is not a string")
		}
		if _, exists := seen[field]; exists {
			return fileObject{}, errors.New("duplicate file field")
		}
		seen[field] = struct{}{}
		switch field {
		case "Blocks":
			if err := decodeJSONContainer(decoder, json.Delim('['), 2, "file blocks"); err != nil {
				return fileObject{}, err
			}
			value.Blocks = make([]string, 0)
			for decoder.More() {
				if len(value.Blocks) == _maxBlocks {
					return fileObject{}, ErrPayloadTooLarge
				}
				block, err := decodeJSONString(decoder, "block id", 3)
				if err != nil {
					return fileObject{}, err
				}
				value.Blocks = append(value.Blocks, block)
			}
			if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
				return fileObject{}, errors.New("invalid file blocks")
			}
		case "Size":
			value.Size, err = decodeJSONString(decoder, "file size", 2)
			if err != nil {
				return fileObject{}, err
			}
		case "Type":
			value.Type, err = decodeJSONString(decoder, "file type", 2)
			if err != nil {
				return fileObject{}, err
			}
		case "Version":
			value.Version, err = decodeJSONInt(decoder, "file version", 2)
			if err != nil {
				return fileObject{}, err
			}
		default:
			return fileObject{}, errors.New("unknown file field")
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') || len(seen) != 4 {
		return fileObject{}, errors.New("invalid file object")
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return fileObject{}, errors.New("trailing JSON data")
	}
	return value, nil
}

func checkFileBudgets(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decodeJSONContainer(decoder, json.Delim('{'), 1, "file object"); err != nil {
		return err
	}
	seen := make(map[string]struct{}, 4)
	for decoder.More() {
		fieldToken, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode file budget field: %w", err)
		}
		field, ok := fieldToken.(string)
		if !ok {
			return errors.New("file field is not a string")
		}
		if _, exists := seen[field]; exists {
			return errors.New("duplicate file field")
		}
		seen[field] = struct{}{}
		switch field {
		case "Blocks":
			if err := decodeJSONContainer(decoder, json.Delim('['), 2, "file blocks"); err != nil {
				return err
			}
			count := 0
			for decoder.More() {
				if count == _maxBlocks {
					return ErrPayloadTooLarge
				}
				if _, err := decodeJSONString(decoder, "block id", 3); err != nil {
					return err
				}
				count++
			}
			if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
				return errors.New("invalid file blocks")
			}
		case "Size":
			size, err := decodeJSONString(decoder, "file size", 2)
			if err != nil {
				return err
			}
			if _, err := parseDecimal(size, _maxFileSize); err != nil {
				return err
			}
		case "Type", "Version":
			if err := consumeJSONValue(decoder, 2); err != nil {
				return err
			}
		default:
			return errors.New("unknown file field")
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return errors.New("invalid file object")
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return errors.New("trailing JSON data")
	}
	return nil
}

func canonicalFile(data []byte) ([]byte, error) {
	value, err := decodeFile(data)
	if err != nil {
		return nil, err
	}
	if value.Type != "File" || value.Version != 1 || value.Blocks == nil {
		return nil, errors.New("invalid file object")
	}
	size, err := parseDecimal(value.Size, _maxFileSize)
	if err != nil {
		return nil, err
	}
	for _, block := range value.Blocks {
		if !ValidID(block) {
			return nil, errors.New("invalid block id")
		}
	}
	minimum := int64(0)
	maximum := int64(0)
	if len(value.Blocks) > 0 {
		minimum = int64(len(value.Blocks)-1)*MaxBlockSize + 1
		maximum = int64(len(value.Blocks)) * MaxBlockSize
	}
	if size < minimum || size > maximum {
		return nil, errors.New("file size does not match block count")
	}
	return appendFile(nil, value), nil
}

type directoryObject struct {
	Entries []directoryEntry `json:"Entries"`
	Type    string           `json:"Type"`
	Version int              `json:"Version"`
}

type directoryEntry struct {
	ID         string `json:"Id"`
	ModifiedAt string `json:"ModifiedAt"`
	Name       string `json:"Name"`
	Type       string `json:"Type"`
}

func decodeDirectory(data []byte) (directoryObject, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decodeJSONContainer(decoder, json.Delim('{'), 1, "directory object"); err != nil {
		return directoryObject{}, err
	}
	var value directoryObject
	seen := make(map[string]struct{}, 3)
	for decoder.More() {
		fieldToken, err := decoder.Token()
		if err != nil {
			return directoryObject{}, fmt.Errorf("decode directory field: %w", err)
		}
		field, ok := fieldToken.(string)
		if !ok {
			return directoryObject{}, errors.New("directory field is not a string")
		}
		if _, exists := seen[field]; exists {
			return directoryObject{}, errors.New("duplicate directory field")
		}
		seen[field] = struct{}{}
		switch field {
		case "Entries":
			if err := decodeJSONContainer(decoder, json.Delim('['), 2, "directory entries"); err != nil {
				return directoryObject{}, err
			}
			value.Entries = make([]directoryEntry, 0)
			for decoder.More() {
				if len(value.Entries) == _maxEntries {
					return directoryObject{}, ErrPayloadTooLarge
				}
				entry, err := decodeDirectoryEntry(decoder)
				if err != nil {
					return directoryObject{}, err
				}
				value.Entries = append(value.Entries, entry)
			}
			if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
				return directoryObject{}, errors.New("invalid directory entries")
			}
		case "Type":
			value.Type, err = decodeJSONString(decoder, "directory type", 2)
			if err != nil {
				return directoryObject{}, err
			}
		case "Version":
			value.Version, err = decodeJSONInt(decoder, "directory version", 2)
			if err != nil {
				return directoryObject{}, err
			}
		default:
			return directoryObject{}, errors.New("unknown directory field")
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') || len(seen) != 3 {
		return directoryObject{}, errors.New("invalid directory object")
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return directoryObject{}, errors.New("trailing JSON data")
	}
	return value, nil
}

func decodeDirectoryEntry(decoder *json.Decoder) (directoryEntry, error) {
	if err := decodeJSONContainer(decoder, json.Delim('{'), 3, "directory entry"); err != nil {
		return directoryEntry{}, err
	}
	var entry directoryEntry
	seen := make(map[string]struct{}, 4)
	for decoder.More() {
		fieldToken, err := decoder.Token()
		if err != nil {
			return directoryEntry{}, fmt.Errorf("decode directory entry field: %w", err)
		}
		field, ok := fieldToken.(string)
		if !ok {
			return directoryEntry{}, errors.New("directory entry field is not a string")
		}
		if _, exists := seen[field]; exists {
			return directoryEntry{}, errors.New("duplicate directory entry field")
		}
		seen[field] = struct{}{}
		var target *string
		switch field {
		case "Id":
			target = &entry.ID
		case "ModifiedAt":
			target = &entry.ModifiedAt
		case "Name":
			target = &entry.Name
		case "Type":
			target = &entry.Type
		default:
			return directoryEntry{}, errors.New("unknown directory entry field")
		}
		*target, err = decodeJSONString(decoder, "directory entry value", 4)
		if err != nil {
			return directoryEntry{}, err
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') || len(seen) != 4 {
		return directoryEntry{}, errors.New("invalid directory entry object")
	}
	return entry, nil
}

func canonicalDirectory(data []byte) ([]byte, error) {
	value, err := decodeDirectory(data)
	if err != nil {
		return nil, err
	}
	if value.Type != "Directory" || value.Version != 1 || value.Entries == nil {
		return nil, errors.New("invalid directory object")
	}
	seen := make(map[string]struct{}, len(value.Entries))
	previous := ""
	for i, entry := range value.Entries {
		if !ValidID(entry.ID) || (entry.Type != "File" && entry.Type != "Directory") || !validTimestamp(entry.ModifiedAt) || !validName(entry.Name) {
			return nil, errors.New("invalid directory entry")
		}
		if i > 0 && strings.Compare(previous, entry.Name) >= 0 {
			return nil, errors.New("directory entries are not sorted")
		}
		previous = entry.Name
		key := cases.Fold().String(entry.Name)
		if _, exists := seen[key]; exists {
			return nil, errors.New("directory name collision")
		}
		seen[key] = struct{}{}
	}
	return appendDirectory(nil, value), nil
}

type commitObject struct {
	AuthorUserID string   `json:"AuthorUserId"`
	CreatedAt    string   `json:"CreatedAt"`
	DeviceID     string   `json:"DeviceId"`
	Message      *string  `json:"Message"`
	Parents      []string `json:"Parents"`
	Root         string   `json:"Root"`
	Type         string   `json:"Type"`
	Version      int      `json:"Version"`
}

func canonicalCommit(data []byte) ([]byte, error) {
	value, err := decodeCommit(data)
	if err != nil {
		return nil, err
	}
	return appendCommit(nil, value), nil
}

func decodeCommit(data []byte) (commitObject, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return commitObject{}, fmt.Errorf("decode commit: %w", err)
	}
	if token != json.Delim('{') {
		if delim, ok := token.(json.Delim); ok {
			if err := consumeJSONContainer(decoder, delim, 1); err != nil {
				return commitObject{}, err
			}
		}
		return commitObject{}, errors.New("commit object must be an object")
	}

	var value commitObject
	seen := make(map[string]struct{}, 8)
	for decoder.More() {
		fieldToken, err := decoder.Token()
		if err != nil {
			return commitObject{}, fmt.Errorf("decode commit field: %w", err)
		}
		field, ok := fieldToken.(string)
		if !ok {
			return commitObject{}, errors.New("commit field is not a string")
		}
		if _, exists := seen[field]; exists {
			return commitObject{}, errors.New("duplicate commit field")
		}
		seen[field] = struct{}{}

		switch field {
		case "AuthorUserId":
			value.AuthorUserID, err = decodeCommitString(decoder, field, 2)
			if err == nil && !validUUID(value.AuthorUserID) {
				err = errors.New("invalid commit author")
			}
		case "CreatedAt":
			value.CreatedAt, err = decodeCommitString(decoder, field, 2)
			if err == nil && !validTimestamp(value.CreatedAt) {
				err = errors.New("invalid commit timestamp")
			}
		case "DeviceId":
			value.DeviceID, err = decodeCommitString(decoder, field, 2)
			if err == nil && !validUUID(value.DeviceID) {
				err = errors.New("invalid commit device")
			}
		case "Message":
			var message string
			message, err = decodeCommitString(decoder, field, 2)
			if err == nil {
				value.Message = &message
			}
		case "Parents":
			value.Parents, err = decodeCommitParents(decoder)
		case "Root":
			value.Root, err = decodeCommitString(decoder, field, 2)
			if err == nil && !ValidID(value.Root) {
				err = errors.New("invalid commit root")
			}
		case "Type":
			value.Type, err = decodeCommitString(decoder, field, 2)
			if err == nil && value.Type != "Commit" {
				err = errors.New("invalid commit type")
			}
		case "Version":
			value.Version, err = decodeCommitVersion(decoder)
		default:
			return commitObject{}, errors.New("unknown commit field")
		}
		if err != nil {
			return commitObject{}, err
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') || len(seen) != 8 {
		return commitObject{}, errors.New("invalid commit object")
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return commitObject{}, errors.New("trailing JSON data")
	}
	return value, nil
}

func decodeJSONContainer(decoder *json.Decoder, expected json.Delim, depth int, description string) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode %s: %w", description, err)
	}
	if token == expected {
		if depth > _maxJSONNesting {
			return ErrPayloadTooLarge
		}
		return nil
	}
	if delim, ok := token.(json.Delim); ok {
		if err := consumeJSONContainer(decoder, delim, depth); err != nil {
			return err
		}
	}
	return fmt.Errorf("invalid %s", description)
}

func decodeJSONString(decoder *json.Decoder, description string, containerDepth int) (string, error) {
	token, err := decoder.Token()
	if err != nil {
		return "", fmt.Errorf("decode %s: %w", description, err)
	}
	if value, ok := token.(string); ok {
		return value, nil
	}
	if delim, ok := token.(json.Delim); ok {
		if err := consumeJSONContainer(decoder, delim, containerDepth); err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("%s must be a string", description)
}

func decodeJSONInt(decoder *json.Decoder, description string, containerDepth int) (int, error) {
	token, err := decoder.Token()
	if err != nil {
		return 0, fmt.Errorf("decode %s: %w", description, err)
	}
	if number, ok := token.(json.Number); ok {
		value, err := strconv.Atoi(number.String())
		if err == nil {
			return value, nil
		}
	}
	if delim, ok := token.(json.Delim); ok {
		if err := consumeJSONContainer(decoder, delim, containerDepth); err != nil {
			return 0, err
		}
	}
	return 0, fmt.Errorf("invalid %s", description)
}

func decodeCommitString(decoder *json.Decoder, field string, containerDepth int) (string, error) {
	return decodeJSONString(decoder, "commit "+field, containerDepth)
}

func decodeCommitVersion(decoder *json.Decoder) (int, error) {
	token, err := decoder.Token()
	if err != nil {
		return 0, fmt.Errorf("decode commit Version: %w", err)
	}
	if number, ok := token.(json.Number); ok && number.String() == "1" {
		return 1, nil
	}
	if delim, ok := token.(json.Delim); ok {
		if err := consumeJSONContainer(decoder, delim, 2); err != nil {
			return 0, err
		}
	}
	return 0, errors.New("invalid commit version")
}

func decodeCommitParents(decoder *json.Decoder) ([]string, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("decode commit Parents: %w", err)
	}
	if token != json.Delim('[') {
		if delim, ok := token.(json.Delim); ok {
			if err := consumeJSONContainer(decoder, delim, 2); err != nil {
				return nil, err
			}
		}
		return nil, errors.New("commit Parents must be an array")
	}

	parents := make([]string, 0, _maxParents)
	for decoder.More() {
		if len(parents) == _maxParents {
			return nil, ErrPayloadTooLarge
		}
		parent, err := decodeCommitString(decoder, "Parents item", 3)
		if err != nil {
			return nil, err
		}
		if !ValidID(parent) {
			return nil, errors.New("invalid parent id")
		}
		parents = append(parents, parent)
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
		return nil, errors.New("invalid commit Parents")
	}
	return parents, nil
}

func consumeJSONValue(decoder *json.Decoder, depth int) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON value: %w", err)
	}
	if delim, ok := token.(json.Delim); ok {
		return consumeJSONContainer(decoder, delim, depth)
	}
	return nil
}

func consumeJSONContainer(decoder *json.Decoder, open json.Delim, depth int) error {
	if depth > _maxJSONNesting {
		return ErrPayloadTooLarge
	}
	if open != json.Delim('{') && open != json.Delim('[') {
		return errors.New("invalid JSON delimiter")
	}
	for decoder.More() {
		if open == json.Delim('{') {
			if token, err := decoder.Token(); err != nil {
				return fmt.Errorf("decode JSON key: %w", err)
			} else if _, ok := token.(string); !ok {
				return errors.New("JSON object key is not a string")
			}
		}
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode JSON value: %w", err)
		}
		if delim, ok := token.(json.Delim); ok {
			if err := consumeJSONContainer(decoder, delim, depth+1); err != nil {
				return err
			}
		}
	}
	closing, err := decoder.Token()
	if err != nil || (open == json.Delim('{') && closing != json.Delim('}')) || (open == json.Delim('[') && closing != json.Delim(']')) {
		return errors.New("invalid JSON structure")
	}
	return nil
}

func parseDecimal(value string, maximum int64) (int64, error) {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return 0, errors.New("invalid decimal string")
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 || strconv.FormatInt(parsed, 10) != value {
		return 0, errors.New("invalid decimal string")
	}
	if parsed > maximum {
		return 0, ErrPayloadTooLarge
	}
	return parsed, nil
}

func validTimestamp(value string) bool {
	parsed, err := time.Parse("2006-01-02T15:04:05Z", value)
	return err == nil && parsed.UTC().Format("2006-01-02T15:04:05Z") == value
}

func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	if err != nil || len(decoded) != 16 {
		return false
	}
	version := decoded[6] >> 4
	return version >= 1 && version <= 8 && decoded[8]&0xc0 == 0x80
}

func validName(name string) bool {
	if name == "" || name != norm.NFC.String(name) || len(name) > 240 || name == "." || name == ".." ||
		strings.HasPrefix(name, ".filecloud-internal-") || strings.HasSuffix(name, " ") || strings.HasSuffix(name, ".") {
		return false
	}
	for _, r := range name {
		if r <= 0x1f || strings.ContainsRune(`<>:"/\\|?*`, r) {
			return false
		}
	}
	base := name
	if index := strings.IndexByte(base, '.'); index >= 0 {
		base = base[:index]
	}
	upper := strings.ToUpper(base)
	if upper == "CON" || upper == "PRN" || upper == "AUX" || upper == "NUL" ||
		(len(upper) == 4 && (strings.HasPrefix(upper, "COM") || strings.HasPrefix(upper, "LPT")) && upper[3] >= '1' && upper[3] <= '9') {
		return false
	}
	return true
}

func appendFile(dst []byte, value fileObject) []byte {
	dst = append(dst, `{"Blocks":[`...)
	for i, block := range value.Blocks {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONString(dst, block)
	}
	dst = append(dst, `],"Size":`...)
	dst = appendJSONString(dst, value.Size)
	dst = append(dst, `,"Type":"File","Version":1}`...)
	return dst
}

func appendDirectory(dst []byte, value directoryObject) []byte {
	dst = append(dst, `{"Entries":[`...)
	for i, entry := range value.Entries {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, `{"Id":`...)
		dst = appendJSONString(dst, entry.ID)
		dst = append(dst, `,"ModifiedAt":`...)
		dst = appendJSONString(dst, entry.ModifiedAt)
		dst = append(dst, `,"Name":`...)
		dst = appendJSONString(dst, entry.Name)
		dst = append(dst, `,"Type":`...)
		dst = appendJSONString(dst, entry.Type)
		dst = append(dst, '}')
	}
	dst = append(dst, `],"Type":"Directory","Version":1}`...)
	return dst
}

func appendCommit(dst []byte, value commitObject) []byte {
	dst = append(dst, `{"AuthorUserId":`...)
	dst = appendJSONString(dst, value.AuthorUserID)
	dst = append(dst, `,"CreatedAt":`...)
	dst = appendJSONString(dst, value.CreatedAt)
	dst = append(dst, `,"DeviceId":`...)
	dst = appendJSONString(dst, value.DeviceID)
	dst = append(dst, `,"Message":`...)
	dst = appendJSONString(dst, *value.Message)
	dst = append(dst, `,"Parents":[`...)
	for i, parent := range value.Parents {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONString(dst, parent)
	}
	dst = append(dst, `],"Root":`...)
	dst = appendJSONString(dst, value.Root)
	dst = append(dst, `,"Type":"Commit","Version":1}`...)
	return dst
}

func appendJSONString(dst []byte, value string) []byte {
	dst = append(dst, '"')
	for _, r := range value {
		switch r {
		case '"', '\\':
			dst = append(dst, '\\', byte(r))
		case '\b':
			dst = append(dst, `\b`...)
		case '\t':
			dst = append(dst, `\t`...)
		case '\n':
			dst = append(dst, `\n`...)
		case '\f':
			dst = append(dst, `\f`...)
		case '\r':
			dst = append(dst, `\r`...)
		default:
			if r < 0x20 {
				dst = append(dst, `\u00`...)
				dst = append(dst, "0123456789abcdef"[r>>4], "0123456789abcdef"[r&15])
			} else {
				dst = utf8.AppendRune(dst, r)
			}
		}
	}
	return append(dst, '"')
}

func validEscapedSurrogates(data []byte) bool {
	for i := 0; i < len(data); i++ {
		if data[i] != '\\' || i+1 >= len(data) {
			continue
		}
		i++
		if data[i] != 'u' || i+4 >= len(data) {
			continue
		}
		value, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if value >= 0xdc00 && value <= 0xdfff {
			return false
		}
		if value < 0xd800 || value > 0xdbff {
			continue
		}
		if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}
