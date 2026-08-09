// Package auth implements local passwords and revocable HTTP sessions.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/mingming-cn/filecloud/internal/storage"
	"golang.org/x/crypto/argon2"
)

const (
	_maxRequestBody        = 8 << 10
	_sessionLifetime       = 30 * 24 * time.Hour
	_defaultGlobalKDFLimit = 2
	_defaultSourceKDFLimit = 1
	_defaultUserKDFLimit   = 1
	_maxArgonMemory        = 64 * 1024
	_maxArgonIterations    = 10
	_maxArgonParallelism   = 16
	_minArgonSaltLength    = 8
	_maxArgonSaltLength    = 64
	_minArgonKeyLength     = 16
	_maxArgonKeyLength     = 64
	_accessTokenBytes      = 32
	_maxEncodedHashLength  = 256
)

var (
	_errInvalidHash        = errors.New("invalid password hash")
	_errInvalidParams      = errors.New("invalid argon2 parameters")
	_errHashParamsMismatch = errors.New("password hash parameters do not match configuration")
)

// Params are the versioned Argon2id work parameters.
type Params struct {
	Memory      uint32
	Iterations  uint32
	Parallelism uint8
	SaltLength  uint32
	KeyLength   uint32
}

// DefaultParams returns the production password parameters.
func DefaultParams() Params {
	return Params{Memory: 64 * 1024, Iterations: 3, Parallelism: 2, SaltLength: 16, KeyLength: 32}
}

// HashPassword creates a parameter-versioned Argon2id hash.
func HashPassword(password []byte, params Params, random io.Reader) (string, error) {
	if err := validateParams(params); err != nil {
		return "", err
	}
	if random == nil {
		random = rand.Reader
	}
	salt := make([]byte, params.SaltLength)
	if _, err := io.ReadFull(random, salt); err != nil {
		return "", fmt.Errorf("generate password salt: %w", err)
	}
	key := argon2.IDKey(password, salt, params.Iterations, params.Memory, params.Parallelism, params.KeyLength)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s", argon2.Version,
		params.Memory, params.Iterations, params.Parallelism,
		base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(key)), nil
}

// VerifyPassword verifies encodedHash only when all embedded parameters match expected.
func VerifyPassword(password []byte, encodedHash string, expected Params) (bool, error) {
	if err := validateParams(expected); err != nil {
		return false, err
	}
	if len(encodedHash) > _maxEncodedHashLength {
		return false, _errInvalidHash
	}
	parts := strings.Split(encodedHash, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" || parts[2] != "v="+strconv.Itoa(argon2.Version) {
		return false, _errInvalidHash
	}
	params, err := parseParams(parts[3])
	if err != nil || validateKDFParams(params) != nil {
		return false, _errInvalidHash
	}
	if len(parts[4]) > base64.RawStdEncoding.EncodedLen(_maxArgonSaltLength) ||
		len(parts[5]) > base64.RawStdEncoding.EncodedLen(_maxArgonKeyLength) {
		return false, _errInvalidHash
	}
	salt, err := base64.RawStdEncoding.Strict().DecodeString(parts[4])
	if err != nil || base64.RawStdEncoding.EncodeToString(salt) != parts[4] {
		return false, _errInvalidHash
	}
	want, err := base64.RawStdEncoding.Strict().DecodeString(parts[5])
	if err != nil || base64.RawStdEncoding.EncodeToString(want) != parts[5] {
		return false, _errInvalidHash
	}
	params.SaltLength = uint32(len(salt))
	params.KeyLength = uint32(len(want))
	if validateParams(params) != nil {
		return false, _errInvalidHash
	}
	if params != expected {
		return false, _errHashParamsMismatch
	}
	got := argon2.IDKey(password, salt, expected.Iterations, expected.Memory, expected.Parallelism, expected.KeyLength)
	return subtle.ConstantTimeCompare(got, want) == 1, nil
}

func validateParams(params Params) error {
	if err := validateKDFParams(params); err != nil {
		return err
	}
	if params.SaltLength < _minArgonSaltLength || params.SaltLength > _maxArgonSaltLength ||
		params.KeyLength < _minArgonKeyLength || params.KeyLength > _maxArgonKeyLength {
		return _errInvalidParams
	}
	return nil
}

func validateKDFParams(params Params) error {
	if params.Memory < 8*uint32(params.Parallelism) || params.Memory > _maxArgonMemory ||
		params.Iterations < 1 || params.Iterations > _maxArgonIterations ||
		params.Parallelism < 1 || params.Parallelism > _maxArgonParallelism {
		return _errInvalidParams
	}
	return nil
}

func parseParams(encoded string) (Params, error) {
	parts := strings.Split(encoded, ",")
	if len(parts) != 3 {
		return Params{}, _errInvalidHash
	}
	memory, err := parseUint(parts[0], "m=", 32)
	if err != nil {
		return Params{}, _errInvalidHash
	}
	iterations, err := parseUint(parts[1], "t=", 32)
	if err != nil {
		return Params{}, _errInvalidHash
	}
	parallelism, err := parseUint(parts[2], "p=", 8)
	if err != nil {
		return Params{}, _errInvalidHash
	}
	return Params{
		Memory:      uint32(memory),
		Iterations:  uint32(iterations),
		Parallelism: uint8(parallelism),
	}, nil
}

func parseUint(encoded, prefix string, bitSize int) (uint64, error) {
	valueText, ok := strings.CutPrefix(encoded, prefix)
	if !ok || valueText == "" {
		return 0, _errInvalidHash
	}
	value, err := strconv.ParseUint(valueText, 10, bitSize)
	if err != nil || strconv.FormatUint(value, 10) != valueText {
		return 0, _errInvalidHash
	}
	return value, nil
}

// NewUserID returns a random RFC 9562 UUIDv4 string.
func NewUserID(random io.Reader) (string, error) {
	if random == nil {
		random = rand.Reader
	}
	var id [16]byte
	if _, err := io.ReadFull(random, id[:]); err != nil {
		return "", fmt.Errorf("generate user id: %w", err)
	}
	id[6] = id[6]&0x0f | 0x40
	id[8] = id[8]&0x3f | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(id[0:4]), hex.EncodeToString(id[4:6]),
		hex.EncodeToString(id[6:8]), hex.EncodeToString(id[8:10]), hex.EncodeToString(id[10:16])), nil
}

// ValidateUsername applies the documented character bounds after NFC normalization.
func ValidateUsername(username string) (string, error) {
	if !utf8.ValidString(username) {
		return "", errors.New("username is not valid utf-8")
	}
	display, _ := storage.CanonicalUsername(username)
	count := utf8.RuneCountInString(display)
	if count < 1 || count > 128 {
		return "", errors.New("username must be 1-128 characters")
	}
	return display, nil
}

// ValidatePassword applies the documented byte bounds.
func ValidatePassword(password []byte) error {
	if len(password) < 1 || len(password) > 1024 {
		return errors.New("password must be 1-1024 bytes")
	}
	return nil
}

// HandlerConfig contains bounded capacities and deterministic construction seams.
type HandlerConfig struct {
	GlobalKDFLimit   int
	SourceIPKDFLimit int
	UsernameKDFLimit int
	Params           Params
	Random           io.Reader
	Now              func() time.Time
	limiter          *kdfLimiter
}

// NewHandler constructs the session endpoints.
func NewHandler(store *storage.Store, logger *log.Logger, config HandlerConfig) (http.Handler, error) {
	if store == nil {
		return nil, errors.New("authentication store is required")
	}
	if logger == nil {
		logger = log.Default()
	}
	if config.Params == (Params{}) {
		config.Params = DefaultParams()
	}
	if err := validateParams(config.Params); err != nil {
		return nil, err
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.limiter == nil {
		limits := [3]int{config.GlobalKDFLimit, config.SourceIPKDFLimit, config.UsernameKDFLimit}
		defaults := [3]int{_defaultGlobalKDFLimit, _defaultSourceKDFLimit, _defaultUserKDFLimit}
		for i := range limits {
			if limits[i] == 0 {
				limits[i] = defaults[i]
			}
		}
		var err error
		config.limiter, err = newKDFLimiter(limits[0], limits[1], limits[2])
		if err != nil {
			return nil, err
		}
	}
	dummy, err := HashPassword([]byte("filecloud dummy password"), config.Params, config.Random)
	if err != nil {
		return nil, err
	}
	h := &handler{store: store, logger: logger, config: config, dummyHash: dummy}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sessions", h.createSession)
	mux.HandleFunc("DELETE /v1/sessions/current", h.deleteCurrentSession)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		mux.ServeHTTP(w, r)
	}), nil
}

type kdfLimiter struct {
	mu            sync.Mutex
	globalLimit   int
	sourceLimit   int
	usernameLimit int
	global        int
	sources       map[string]int
	usernames     map[string]int
}

func newKDFLimiter(globalLimit, sourceLimit, usernameLimit int) (*kdfLimiter, error) {
	if globalLimit < 1 || sourceLimit < 1 || usernameLimit < 1 {
		return nil, errors.New("kdf concurrency limits must be positive")
	}
	return &kdfLimiter{
		globalLimit: globalLimit, sourceLimit: sourceLimit, usernameLimit: usernameLimit,
		sources: make(map[string]int), usernames: make(map[string]int),
	}, nil
}

func (l *kdfLimiter) tryAcquire(source, username string) (func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.global >= l.globalLimit || l.sources[source] >= l.sourceLimit || l.usernames[username] >= l.usernameLimit {
		return nil, false
	}
	l.global++
	l.sources[source]++
	l.usernames[username]++
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.global--
			decrement(l.sources, source)
			decrement(l.usernames, username)
		})
	}, true
}

func decrement(counts map[string]int, key string) {
	if counts[key] == 1 {
		delete(counts, key)
		return
	}
	counts[key]--
}

type handler struct {
	store     *storage.Store
	logger    *log.Logger
	config    HandlerConfig
	dummyHash string
}

type userIDContextKey struct{}

// RequireSession authenticates bearer tokens before invoking next.
func RequireSession(store *storage.Store, now func() time.Time, logger *log.Logger, next http.Handler) http.Handler {
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = log.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		if !ok || scheme != "Bearer" || !validAccessToken(token) {
			writeAuthenticationError(w, logger)
			return
		}
		userID, err := store.FindActiveSession(r.Context(), sha256.Sum256([]byte(token)), now().UTC())
		if errors.Is(err, storage.ErrSessionNotFound) {
			writeAuthenticationError(w, logger)
			return
		}
		if err != nil {
			logger.Printf("authenticate session: %v", err)
			writeAuthenticationJSON(w, http.StatusInternalServerError, 5000, "internal error", logger)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userIDContextKey{}, userID)))
	})
}

// UserID returns the authenticated user ID installed by RequireSession.
func UserID(ctx context.Context) (string, bool) {
	userID, ok := ctx.Value(userIDContextKey{}).(string)
	return userID, ok && userID != ""
}

func writeAuthenticationError(w http.ResponseWriter, logger *log.Logger) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	writeAuthenticationJSON(w, http.StatusUnauthorized, 1001, "authentication failed", logger)
}

func writeAuthenticationJSON(w http.ResponseWriter, status, code int, message string, logger *log.Logger) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(struct {
		RetCode int
		Message string
	}{RetCode: code, Message: message}); err != nil {
		logger.Printf("write JSON response: %v", err)
	}
}

type createSessionRequest struct {
	Username   string
	Password   string
	DeviceName string
}

func (h *handler) createSession(w http.ResponseWriter, r *http.Request) {
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" {
		h.writeError(w, http.StatusBadRequest, 1000, "invalid request")
		return
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, _maxRequestBody+1))
	if err != nil {
		h.internal(w, "read session request", err)
		return
	}
	if len(data) > _maxRequestBody {
		h.writeError(w, http.StatusRequestEntityTooLarge, 3005, "request body too large")
		return
	}
	request, err := decodeCreateSession(data)
	if err != nil || validateCreateSession(request) != nil {
		h.writeError(w, http.StatusBadRequest, 1000, "invalid request")
		return
	}
	_, usernameKey := storage.CanonicalUsername(request.Username)
	release, ok := h.config.limiter.tryAcquire(sourceIP(r.RemoteAddr), usernameKey)
	if !ok {
		w.Header().Set("Retry-After", "1")
		h.writeError(w, http.StatusTooManyRequests, 4000, "authentication busy")
		return
	}
	defer release()

	user, findErr := h.store.FindUserByUsername(r.Context(), request.Username)
	if findErr != nil && !errors.Is(findErr, storage.ErrUserNotFound) {
		h.internal(w, "find login user", findErr)
		return
	}
	hash := h.dummyHash
	if findErr == nil {
		hash = user.PasswordHash
	}
	valid, verifyErr := VerifyPassword([]byte(request.Password), hash, h.config.Params)
	if verifyErr != nil {
		if findErr == nil {
			_, dummyErr := VerifyPassword([]byte(request.Password), h.dummyHash, h.config.Params)
			verifyErr = errors.Join(verifyErr, dummyErr)
		}
		h.internal(w, "verify password hash", verifyErr)
		return
	}
	if findErr != nil || !valid {
		h.writeUnauthenticated(w)
		return
	}
	tokenBytes := make([]byte, _accessTokenBytes)
	if _, err := io.ReadFull(h.config.Random, tokenBytes); err != nil {
		h.internal(w, "generate access token", err)
		return
	}
	token := base64.RawURLEncoding.EncodeToString(tokenBytes)
	tokenHash := sha256.Sum256([]byte(token))
	now := h.config.Now().UTC()
	expiresAt := now.Add(_sessionLifetime)
	if err := h.store.CreateSession(r.Context(), user.ID, tokenHash, request.DeviceName, now, expiresAt); err != nil {
		h.internal(w, "persist session", err)
		return
	}
	h.writeJSON(w, http.StatusOK, struct {
		RetCode int
		Message string
		Session struct {
			AccessToken string
			ExpiresAt   string
			UserID      string `json:"UserId"`
		}
	}{RetCode: 0, Message: "success", Session: struct {
		AccessToken string
		ExpiresAt   string
		UserID      string `json:"UserId"`
	}{AccessToken: token, ExpiresAt: expiresAt.Format(time.RFC3339), UserID: user.ID}})
}

func (h *handler) deleteCurrentSession(w http.ResponseWriter, r *http.Request) {
	authorization := r.Header.Get("Authorization")
	scheme, token, ok := strings.Cut(authorization, " ")
	if !ok || scheme != "Bearer" || !validAccessToken(token) {
		h.writeUnauthenticated(w)
		return
	}
	tokenHash := sha256.Sum256([]byte(token))
	revoked, err := h.store.RevokeSession(r.Context(), tokenHash, h.config.Now().UTC())
	if err != nil {
		h.internal(w, "revoke session", err)
		return
	}
	if !revoked {
		h.writeUnauthenticated(w)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func sourceIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		return host
	}
	return remoteAddr
}

func validAccessToken(token string) bool {
	if len(token) != base64.RawURLEncoding.EncodedLen(_accessTokenBytes) {
		return false
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(token)
	return err == nil && len(decoded) == _accessTokenBytes
}

func decodeCreateSession(data []byte) (createSessionRequest, error) {
	if !utf8.Valid(data) {
		return createSessionRequest{}, errors.New("request is not valid utf-8")
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return createSessionRequest{}, errors.New("request must be an object")
	}
	var request createSessionRequest
	seen := make(map[string]bool, 3)
	for decoder.More() {
		nameToken, err := decoder.Token()
		if err != nil {
			return request, err
		}
		name, ok := nameToken.(string)
		if !ok || seen[name] {
			return request, errors.New("invalid or duplicate field")
		}
		seen[name] = true
		var value string
		if err := decoder.Decode(&value); err != nil {
			return request, err
		}
		switch name {
		case "Username":
			request.Username = value
		case "Password":
			request.Password = value
		case "DeviceName":
			request.DeviceName = value
		default:
			return request, errors.New("unknown field")
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return request, errors.New("invalid object")
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return request, errors.New("trailing data")
	}
	return request, nil
}

func validateCreateSession(request createSessionRequest) error {
	if _, err := ValidateUsername(request.Username); err != nil {
		return err
	}
	if err := ValidatePassword([]byte(request.Password)); err != nil {
		return err
	}
	if !utf8.ValidString(request.DeviceName) {
		return errors.New("device name is not valid utf-8")
	}
	count := utf8.RuneCountInString(request.DeviceName)
	if count < 1 || count > 128 {
		return errors.New("device name must be 1-128 characters")
	}
	return nil
}

func (h *handler) writeUnauthenticated(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	h.writeError(w, http.StatusUnauthorized, 1001, "authentication failed")
}

func (h *handler) writeError(w http.ResponseWriter, status, code int, message string) {
	h.writeJSON(w, status, struct {
		RetCode int
		Message string
	}{RetCode: code, Message: message})
}

func (h *handler) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		h.logger.Printf("write JSON response: %v", err)
	}
}

func (h *handler) internal(w http.ResponseWriter, operation string, err error) {
	h.logger.Printf("%s: %v", operation, err)
	h.writeError(w, http.StatusInternalServerError, 5000, "internal error")
}
