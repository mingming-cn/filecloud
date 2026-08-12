package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	libraryapi "github.com/mingming-cn/filecloud/internal/library"
	"github.com/mingming-cn/filecloud/internal/object"
	"github.com/mingming-cn/filecloud/internal/storage"
)

func TestIssue22LinuxExt4AcceptanceMatrix(t *testing.T) {
	if os.Getenv("FILECLOUD_ISSUE22_MATRIX_CHILD") == "1" {
		t.Skip("issue #22 child suite does not recurse")
	}
	if os.Getenv("FILECLOUD_RUN_1A") != "1" {
		t.Skip("set FILECLOUD_RUN_1A=1 to run the Linux/ext4 acceptance matrix")
	}
	ext4Temp, err := os.MkdirTemp(".", ".issue22-matrix-")
	if err != nil {
		t.Fatal(err)
	}
	ext4Temp, err = filepath.Abs(ext4Temp)
	if err != nil {
		os.RemoveAll(ext4Temp)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(ext4Temp); err != nil {
			t.Errorf("remove issue #22 matrix directory: %v", err)
		}
	})
	requireIssue22Ext4(t, ext4Temp)

	scenarios := []issue22MatrixScenario{
		{category: "correctness oracle", packagePath: "./cmd/filecloud", test: "TestIssue22LinuxExt4CorrectnessLoop"},
		{category: "first binding", packagePath: "./cmd/filecloud", test: "TestLibraryBindDoubleEmptyConvergesAndUnbindIsLocalOnly"},
		{category: "first binding", packagePath: "./cmd/filecloud", test: "TestLibraryBindConcurrentInitializationAdoptsWinner"},
		{category: "first binding", packagePath: "./cmd/filecloud", test: "TestLibraryBindImportsLocalSnapshotAndSyncNoOps"},
		{category: "first binding", packagePath: "./cmd/filecloud", test: "TestLibraryBindChecksOutRemoteHeadWithoutMutation"},
		{category: "first binding", packagePath: "./cmd/filecloud", test: "TestLibraryBindCheckoutRejectsBothNonEmptyWithoutMutation"},
		{category: "scan races", packagePath: "./cmd/filecloud", test: "TestScanRegularFileRetriesConcurrentRewrite"},
		{category: "scan races", packagePath: "./cmd/filecloud", test: "TestScanDirectoryEnumerationChangeFailsRound"},
		{category: "scan races", packagePath: "./cmd/filecloud", test: "TestScanFinalValidationCatchesEarlierFileChange"},
		{category: "scan races", packagePath: "./cmd/filecloud", test: "TestSyncUnstableScanDoesNotPublishOrChangeClientState"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestFSActionSubprocessCrashMatrix"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestPublicInitialCheckoutBaseCommitCrashMatrix"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestPublicSyncTransactionCrashMatrix"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestFSActionRemoveIntentPreservesOldFDModification"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestLibrarySyncConflictPromotionPreservesOpenFileIdentity"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestLibrarySyncRemoteApplyPreservesOpenFileModification"},
		{category: "server fault injection", packagePath: "./internal/storage", test: "TestObjectStorePublicationCrashMatrixPreservesOldHead"},
		{category: "server fault injection", packagePath: "./internal/library", test: "TestHeadUpdateFaultPointsPreserveReadableInvariant"},
		{category: "transfer recovery", packagePath: "./cmd/filecloud", test: "TestLibrarySyncInterrupted100MiBUploadSendsOnlyMissingBlocks"},
		{category: "transfer recovery", packagePath: "./cmd/filecloud", test: "TestLibraryBindCheckoutDownloadAndDiskFailuresRetryFixedTarget"},
		{category: "transfer recovery", packagePath: "./internal/library", test: "TestBlockResponseLossReplayReturnsCreatedFalse"},
		{category: "permissions", packagePath: "./internal/library", test: "TestLibraryAuthenticationAndOwnerIsolationAreUniform"},
		{category: "permissions", packagePath: "./internal/library", test: "TestCheckObjectsReturnsOnlyOwnerLibraryMissingObjects"},
		{category: "resource limits", packagePath: "./internal/auth", test: "TestCreateSessionEachKDFCapacityReturnsImmediately"},
		{category: "resource limits", packagePath: "./internal/library", test: "TestObjectPUTRejectsBusyRequestsBeforeReadingBody"},
		{category: "resource limits", packagePath: "./internal/library", test: "TestObjectPUTRollsBudgetAcrossRestartWithoutChargingReplays"},
		{category: "resource limits", packagePath: "./internal/library", test: "TestHeadValidationAdmissionBoundsConcurrentWork"},
		{category: "resource limits", packagePath: "./internal/library", test: "TestHeadValidationCommitBudgetBoundaries"},
		{category: "deletion confirmation", packagePath: "./cmd/filecloud", test: "TestLibrarySyncProtectedDeletionConfirmation"},
		{category: "deletion confirmation", packagePath: "./cmd/filecloud", test: "TestLibrarySyncProtectedDeletionMutationRequiresNewCandidate"},
		{category: "deletion confirmation", packagePath: "./cmd/filecloud", test: "TestLibrarySyncConfirmedDeletionResumesUploadFailure"},
		{category: "raw HTTP contract", packagePath: "./internal/auth", test: "TestSessionRawHTTPHeadersAndEmptyLogout"},
		{category: "raw HTTP contract", packagePath: "./internal/library", test: "TestCreateReplayAndReadLibrary"},
		{category: "raw HTTP contract", packagePath: "./internal/library", test: "TestListLibrariesUsesStableOpaqueExpiringPagination"},
		{category: "raw HTTP contract", packagePath: "./internal/library", test: "TestHeadHTTPPreconditionsAndConditionalGet"},
		{category: "raw HTTP contract", packagePath: "./internal/library", test: "TestMetadataObjectHTTPContract"},
		{category: "raw HTTP contract", packagePath: "./internal/library", test: "TestBlockHTTPContract"},
		{category: "raw HTTP contract", packagePath: "./internal/library", test: "TestObjectHTTPWireContract"},
		{category: "raw HTTP contract", packagePath: "./cmd/filecloud", test: "TestLibraryClientIgnoresOptionalResponseFields"},
		{category: "raw HTTP contract", packagePath: "./cmd/filecloud", test: "TestUpdateRemoteHeadRetriesExplicitTransientResponses"},
		{category: "raw HTTP contract", packagePath: "./cmd/filecloud", test: "TestUpdateRemoteHeadDoesNotRetryUnknownNetworkResult"},
		{category: "raw HTTP contract", packagePath: "./cmd/filecloud", test: "TestCheckRemoteObjectsBatchesAt1000"},
	}
	runIssue22Matrix(t, ext4Temp, scenarios)
}

type issue22MatrixScenario struct {
	category    string
	packagePath string
	test        string
}

type issue22TestEvent struct {
	Action  string
	Package string
	Test    string
	Output  string
}

type issue22TestKey struct {
	packagePath string
	test        string
}

func runIssue22Matrix(t *testing.T, ext4Temp string, scenarios []issue22MatrixScenario) {
	t.Helper()
	command := exec.CommandContext(t.Context(), "go", "test", "-json", "./...", "-count=1", "-timeout=10m")
	command.Dir = filepath.Join("..", "..")
	command.Env = append(os.Environ(),
		"TMPDIR="+ext4Temp,
		"FILECLOUD_RUN_1A=1",
		"FILECLOUD_ISSUE22_EXT4_ROOT="+ext4Temp,
		"FILECLOUD_ISSUE22_MATRIX_CHILD=1",
	)
	output, runErr := command.CombinedOutput()
	if runErr != nil {
		t.Fatalf("issue #22 full ext4 suite: %v\n%s", runErr, output)
	}

	const module = "github.com/mingming-cn/filecloud/"
	required := make(map[issue22TestKey]issue22MatrixScenario, len(scenarios))
	for _, scenario := range scenarios {
		packagePath := module + strings.TrimPrefix(scenario.packagePath, "./")
		required[issue22TestKey{packagePath: packagePath, test: scenario.test}] = scenario
	}
	passed := make(map[issue22TestKey]bool, len(required))
	allowedSkips := map[string]bool{
		"TestHeadUpdateCrashHelper":                      true,
		"TestObjectStorePublicationCrashHelper":          true,
		"TestPublicInitialCheckoutBaseCommitCrashHelper": true,
		"TestPublicSyncTransactionCrashHelper":           true,
		"TestPublicUnbindFSActionCrashHelper":            true,
		"TestPublicSyncFSActionCrashHelper":              true,
		"TestPublicFSActionCrashHelper":                  true,
		"TestFSActionCrashHelper":                        true,
		"TestIssue22LinuxExt4AcceptanceMatrix":           true,
	}
	var attestations []string
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var event issue22TestEvent
		err := decoder.Decode(&event)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode issue #22 full-suite event: %v\n%s", err, output)
		}
		if event.Action == "fail" {
			t.Fatalf("issue #22 ext4 test failed: package=%s test=%s\n%s", event.Package, event.Test, output)
		}
		if event.Action == "skip" && !allowedSkips[event.Test] {
			t.Fatalf("issue #22 ext4 test skipped: package=%s test=%s\n%s", event.Package, event.Test, output)
		}
		key := issue22TestKey{packagePath: event.Package, test: event.Test}
		if event.Action == "pass" {
			if _, ok := required[key]; ok {
				passed[key] = true
			}
			if event.Test != "" && !strings.Contains(event.Test, "/") {
				t.Logf("package=%q scenario=%q platform=%s filesystem=ext4 result=pass",
					event.Package, event.Test, runtime.GOOS)
			}
		}
		if event.Test == "TestIssue22LinuxExt4CorrectnessLoop" && event.Action == "output" &&
			strings.Contains(event.Output, "scenario=") {
			attestations = append(attestations, strings.TrimSpace(event.Output))
		}
	}
	for key, scenario := range required {
		if !passed[key] {
			t.Fatalf("issue #22 required scenario did not pass: category=%q package=%q test=%q",
				scenario.category, key.packagePath, key.test)
		}
	}
	assertIssue22Attestations(t, attestations, output)
	for _, attestation := range attestations {
		t.Log(attestation)
	}
}

func assertIssue22Attestations(t *testing.T, attestations []string, output []byte) {
	t.Helper()
	requiredConvergence := map[string]bool{
		"publisher import":                    false,
		"subscriber checkout":                 false,
		"independent merge subscriber":        false,
		"independent merge first client":      false,
		"independent merge second client":     false,
		"conflict preservation first client":  false,
		"conflict preservation second client": false,
	}
	isolationSeen := false
	for _, attestation := range attestations {
		name, ok := issue22AttestationScenario(attestation)
		if !ok {
			t.Fatalf("issue #22 malformed attestation %q\n%s", attestation, output)
		}
		if name == "two-user isolation" {
			if isolationSeen {
				t.Fatalf("issue #22 duplicate isolation attestation %q", attestation)
			}
			for _, field := range []string{"platform=linux", "filesystem=ext4", "other_head=null", "isolation=uniform-not-found"} {
				if !strings.Contains(attestation, field) {
					t.Fatalf("issue #22 isolation attestation missing %q: %q", field, attestation)
				}
			}
			ownerHead, ok := issue22AttestationField(attestation, "owner_head")
			if !ok || !object.ValidID(ownerHead) {
				t.Fatalf("issue #22 isolation attestation has invalid owner Head: %q", attestation)
			}
			isolationSeen = true
			continue
		}
		seen, required := requiredConvergence[name]
		if !required || seen {
			t.Fatalf("issue #22 unexpected or duplicate attestation %q", attestation)
		}
		for _, field := range []string{"platform=linux", "filesystem=ext4"} {
			if !strings.Contains(attestation, field) {
				t.Fatalf("issue #22 convergence attestation missing %q: %q", field, attestation)
			}
		}
		head, headOK := issue22AttestationField(attestation, "Head")
		base, baseOK := issue22AttestationField(attestation, "SyncBase")
		snapshot, snapshotOK := issue22AttestationField(attestation, "snapshot")
		if !headOK || !baseOK || !snapshotOK || !object.ValidID(head) || !object.ValidID(base) ||
			!object.ValidID(snapshot) || head != base {
			t.Fatalf("issue #22 convergence attestation has invalid or divergent IDs: %q", attestation)
		}
		for _, field := range []string{"reachable_objects", "confirmed_inputs"} {
			value, ok := issue22AttestationField(attestation, field)
			count, err := strconv.Atoi(value)
			if !ok || err != nil || count < 1 {
				t.Fatalf("issue #22 convergence attestation has invalid %s: %q", field, attestation)
			}
		}
		internalPaths, ok := issue22AttestationField(attestation, "internal_paths")
		if !ok || internalPaths != "0" {
			t.Fatalf("issue #22 convergence attestation has registered internal paths: %q", attestation)
		}
		requiredConvergence[name] = true
	}
	for name, seen := range requiredConvergence {
		if !seen {
			t.Fatalf("issue #22 correctness oracle emitted no attestation for %q\n%s", name, output)
		}
	}
	if !isolationSeen {
		t.Fatalf("issue #22 correctness oracle emitted no isolation attestation\n%s", output)
	}
}

func issue22AttestationScenario(attestation string) (string, bool) {
	const marker = "scenario=\""
	start := strings.Index(attestation, marker)
	if start < 0 {
		return "", false
	}
	start += len(marker)
	end := strings.IndexByte(attestation[start:], '"')
	if end < 0 {
		return "", false
	}
	return attestation[start : start+end], true
}

func issue22AttestationField(attestation, name string) (string, bool) {
	marker := " " + name + "="
	start := strings.Index(attestation, marker)
	if start < 0 {
		return "", false
	}
	start += len(marker)
	end := strings.IndexByte(attestation[start:], ' ')
	if end < 0 {
		end = len(attestation) - start
	}
	value := attestation[start : start+end]
	return value, value != ""
}

func TestIssue22LinuxExt4CorrectnessLoop(t *testing.T) {
	if os.Getenv("FILECLOUD_RUN_1A") != "1" {
		t.Skip("set FILECLOUD_RUN_1A=1 to run the Linux/ext4 correctness loop")
	}
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	publisherDir, publisherTree := newIssue22ClientPaths(t)
	subscriberDir, subscriberTree := newIssue22ClientPaths(t)

	confirmed := []issue22ConfirmedInput{
		{name: "base", data: []byte("confirmed base")},
	}
	if err := os.WriteFile(filepath.Join(publisherTree, "shared.txt"), confirmed[0].data, 0o600); err != nil {
		t.Fatal(err)
	}
	bindPublisher := append(bindArgs(publisherDir, environment.server.URL, testClientLibraryID, publisherTree, testClientDeviceID), "--import-local")
	if err := runIssue22CLI(t, bindPublisher, environment.token+"\n"); err != nil {
		t.Fatalf("import publisher: %v", err)
	}
	assertIssue22Converged(t, "publisher import", environment, publisherDir, publisherTree, confirmed)

	if err := runIssue22CLI(t, bindArgs(subscriberDir, environment.server.URL, testClientLibraryID, subscriberTree, testOtherDeviceID), environment.token+"\n"); err != nil {
		t.Fatalf("bind subscriber: %v", err)
	}
	assertIssue22Converged(t, "subscriber checkout", environment, subscriberDir, subscriberTree, confirmed)

	remoteOnly := issue22ConfirmedInput{name: "remote independent change", data: []byte("confirmed remote independent")}
	localOnly := issue22ConfirmedInput{name: "local independent change", data: []byte("confirmed local independent")}
	confirmed = append(confirmed, remoteOnly, localOnly)
	if err := os.WriteFile(filepath.Join(publisherTree, "remote.txt"), remoteOnly.data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runIssue22CLI(t, []string{"library", "sync", "--client-dir", publisherDir, "--worktree", publisherTree}, ""); err != nil {
		t.Fatalf("publish remote independent change: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "local.txt"), localOnly.data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runIssue22CLI(t, []string{"library", "sync", "--client-dir", subscriberDir, "--worktree", subscriberTree}, ""); err != nil {
		t.Fatalf("merge independent changes: %v", err)
	}
	assertIssue22Converged(t, "independent merge subscriber", environment, subscriberDir, subscriberTree, confirmed)
	if err := runIssue22CLI(t, []string{"library", "sync", "--client-dir", publisherDir, "--worktree", publisherTree}, ""); err != nil {
		t.Fatalf("download independent merge: %v", err)
	}
	assertIssue22SameConvergence(t, "independent merge", environment,
		issue22Client{clientDir: publisherDir, worktree: publisherTree},
		issue22Client{clientDir: subscriberDir, worktree: subscriberTree}, confirmed)

	remoteConflict := issue22ConfirmedInput{name: "remote conflicting change", data: []byte("confirmed remote conflict")}
	localConflict := issue22ConfirmedInput{name: "local conflicting change", data: []byte("confirmed local conflict")}
	confirmed = append(confirmed, remoteConflict, localConflict)
	if err := os.WriteFile(filepath.Join(publisherTree, "shared.txt"), remoteConflict.data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runIssue22CLI(t, []string{"library", "sync", "--client-dir", publisherDir, "--worktree", publisherTree}, ""); err != nil {
		t.Fatalf("publish remote conflict: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "shared.txt"), localConflict.data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runIssue22CLI(t, []string{"library", "sync", "--client-dir", subscriberDir, "--worktree", subscriberTree}, ""); err != nil {
		t.Fatalf("preserve conflicting changes: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(subscriberTree, "shared.txt")); err != nil || !bytes.Equal(data, remoteConflict.data) {
		t.Fatalf("remote conflict path = %q, %v", data, err)
	}
	matches, err := filepath.Glob(filepath.Join(subscriberTree, "shared (Filecloud conflict *).txt"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("conflict copies = %v, %v", matches, err)
	}
	if data, err := os.ReadFile(matches[0]); err != nil || !bytes.Equal(data, localConflict.data) {
		t.Fatalf("local conflict copy = %q, %v", data, err)
	}
	if err := runIssue22CLI(t, []string{"library", "sync", "--client-dir", publisherDir, "--worktree", publisherTree}, ""); err != nil {
		t.Fatalf("download conflict result: %v", err)
	}
	assertIssue22SameConvergence(t, "conflict preservation", environment,
		issue22Client{clientDir: publisherDir, worktree: publisherTree},
		issue22Client{clientDir: subscriberDir, worktree: subscriberTree}, confirmed)

	assertIssue22OwnerIsolation(t, environment)
}

type issue22ConfirmedInput struct {
	name string
	data []byte
}

type issue22Client struct {
	clientDir string
	worktree  string
}

type issue22Reachability struct {
	objects int
	files   map[string][]byte
}

func runIssue22CLI(t *testing.T, args []string, stdin string) error {
	t.Helper()
	return run(t.Context(), args, bytes.NewBufferString(stdin), io.Discard, io.Discard)
}

func newIssue22ClientPaths(t *testing.T) (string, string) {
	t.Helper()
	clientDir := filepath.Join(t.TempDir(), "client")
	root := os.Getenv("FILECLOUD_ISSUE22_EXT4_ROOT")
	if root == "" {
		root = "."
	}
	worktree, err := os.MkdirTemp(root, ".issue22-worktree-")
	if err != nil {
		t.Fatalf("create ext4 worktree: %v", err)
	}
	canonical, err := filepath.Abs(worktree)
	if err != nil {
		os.RemoveAll(worktree)
		t.Fatalf("canonicalize ext4 worktree: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(canonical); err != nil {
			t.Errorf("remove ext4 worktree: %v", err)
		}
	})
	requireIssue22Ext4(t, canonical)
	return clientDir, canonical
}

func requireIssue22Ext4(t *testing.T, worktree string) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Fatalf("issue #22 requires Linux/ext4, got %s", runtime.GOOS)
	}
	root, err := openWorktreeRoot(worktree, requireExt4)
	if err != nil {
		t.Fatalf("issue #22 requires an ext-family worktree: %v", err)
	}
	defer func() {
		if err := root.Close(); err != nil {
			t.Errorf("close ext4 worktree: %v", err)
		}
	}()
	filesystem, err := mountedFilesystemForDirectory(root.directory)
	if err != nil {
		t.Fatalf("identify worktree filesystem: %v", err)
	}
	var info syscall.Statfs_t
	if err := syscall.Fstatfs(int(root.directory.Fd()), &info); err != nil {
		t.Fatalf("inspect ext4 worktree: %v", err)
	}
	t.Logf("platform=%s filesystem=%s magic=0x%x worktree=%s", runtime.GOOS, filesystem, uint64(info.Type), worktree)
}

func assertIssue22SameConvergence(t *testing.T, scenario string, environment libraryCLIEnvironment,
	first, second issue22Client, confirmed []issue22ConfirmedInput,
) {
	t.Helper()
	firstBinding := assertIssue22Converged(t, scenario+" first client", environment, first.clientDir, first.worktree, confirmed)
	secondBinding := assertIssue22Converged(t, scenario+" second client", environment, second.clientDir, second.worktree, confirmed)
	if firstBinding.SyncBase != secondBinding.SyncBase || firstBinding.SyncBaseRoot != secondBinding.SyncBaseRoot {
		t.Fatalf("%s clients differ: first=%+v second=%+v", scenario, firstBinding, secondBinding)
	}
}

func assertIssue22Converged(t *testing.T, scenario string, environment libraryCLIEnvironment,
	clientDir, worktree string, confirmed []issue22ConfirmedInput,
) clientBinding {
	t.Helper()
	binding := assertTestConverged(t, environment, clientDir, worktree)
	for _, table := range []string{
		"bind_intents", "pending_publications", "pending_checkouts", "checkout_paths", "sync_recoveries",
		"sync_recovery_promotions", "fs_actions",
	} {
		if count := countClientRows(t, clientDir, table, worktree); count != 0 {
			t.Fatalf("%s has %d residual %s rows", scenario, count, table)
		}
	}
	journalBindings := captureTestJournalBindings(t, clientDir, worktree)
	if len(journalBindings) != 1 {
		t.Fatalf("%s has %d filesystem journal root registrations", scenario, len(journalBindings))
	}
	var rootStat syscall.Stat_t
	if err := syscall.Stat(worktree, &rootStat); err != nil {
		t.Fatalf("%s inspect worktree root: %v", scenario, err)
	}
	registered := journalBindings[0]
	if registered.worktree != worktree || registered.rootDevice != uint64(rootStat.Dev) ||
		registered.rootInode != rootStat.Ino || registered.journalFormat != fsJournalFormat {
		t.Fatalf("%s journal root registration=%+v root=%d/%d format=%d", scenario, registered,
			uint64(rootStat.Dev), rootStat.Ino, fsJournalFormat)
	}
	assertNoSyncInternalPaths(t, worktree)

	reachable := inspectIssue22Reachability(t, environment, binding.SyncBase)
	for _, input := range confirmed {
		preserved := false
		for _, data := range reachable.files {
			if bytes.Equal(data, input.data) {
				preserved = true
				break
			}
		}
		if !preserved {
			t.Fatalf("%s lost confirmed input %q", scenario, input.name)
		}
	}
	t.Logf("scenario=%q platform=%s filesystem=ext4 Head=%s SyncBase=%s snapshot=%s reachable_objects=%d confirmed_inputs=%d internal_paths=0",
		scenario, runtime.GOOS, binding.SyncBase, binding.SyncBase, binding.SyncBaseRoot, reachable.objects, len(confirmed))
	return binding
}

func inspectIssue22Reachability(t *testing.T, environment libraryCLIEnvironment, head string) issue22Reachability {
	t.Helper()
	base := mustServerURL(t, environment.server.URL)
	token := []byte(environment.token)
	objects := make(map[string][]byte)
	files := make(map[string][]byte)
	var visitDirectory func(string)
	var visitFile func(string)

	fetch := func(kind, id string) []byte {
		t.Helper()
		key := kind + "\x00" + id
		if data, ok := objects[key]; ok {
			return data
		}
		path := base.JoinPath("v1/libraries", testClientLibraryID, "objects", kind, id).String()
		if kind == "blocks" {
			path = base.JoinPath("v1/libraries", testClientLibraryID, "blocks", id).String()
		}
		request, err := authenticatedRequest(t.Context(), http.MethodGet, path, token, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := noRedirectClient().Do(request)
		if err != nil {
			t.Fatalf("GET reachable %s/%s: %v", kind, id, err)
		}
		data, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("GET reachable %s/%s: status=%d read=%v close=%v", kind, id, response.StatusCode, readErr, closeErr)
		}
		if got := response.Header.Get("ETag"); got != `"`+id+`"` {
			t.Fatalf("GET reachable %s/%s ETag=%q", kind, id, got)
		}
		if got := response.Header.Get("Cache-Control"); got != "private, immutable" {
			t.Fatalf("GET reachable %s/%s Cache-Control=%q", kind, id, got)
		}
		objects[key] = data
		return data
	}

	visitFile = func(id string) {
		if _, ok := files[id]; ok {
			return
		}
		data := fetch("files", id)
		file, err := object.VerifyFile(data, id)
		if err != nil {
			t.Fatalf("verify reachable file %s: %v", id, err)
		}
		content := make([]byte, 0, file.Size)
		for index, blockID := range file.Blocks {
			block := fetch("blocks", blockID)
			if object.ID(block) != blockID || len(block) == 0 || len(block) > object.MaxBlockSize {
				t.Fatalf("reachable block %s has invalid digest or size %d", blockID, len(block))
			}
			if index < len(file.Blocks)-1 && len(block) != object.MaxBlockSize {
				t.Fatalf("reachable non-tail block %s has size %d", blockID, len(block))
			}
			content = append(content, block...)
		}
		if int64(len(content)) != file.Size {
			t.Fatalf("reachable file %s size=%d want=%d", id, len(content), file.Size)
		}
		files[id] = content
	}
	visitDirectory = func(id string) {
		key := "directories\x00" + id
		if _, ok := objects[key]; ok {
			return
		}
		data := fetch("directories", id)
		directory, err := object.VerifyDirectory(data, id)
		if err != nil {
			t.Fatalf("verify reachable directory %s: %v", id, err)
		}
		for _, entry := range directory.Entries {
			switch entry.Type {
			case "Directory":
				visitDirectory(entry.ID)
			case "File":
				visitFile(entry.ID)
			default:
				t.Fatalf("reachable directory %s has invalid entry type %q", id, entry.Type)
			}
		}
	}

	pending := []string{head}
	for len(pending) > 0 {
		commitID := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if _, ok := objects["commits\x00"+commitID]; ok {
			continue
		}
		data := fetch("commits", commitID)
		commit, err := object.VerifyCommit(data, commitID)
		if err != nil || commit.AuthorUserID != testClientUserID {
			t.Fatalf("verify reachable commit %s: commit=%+v err=%v", commitID, commit, err)
		}
		visitDirectory(commit.Root)
		pending = append(pending, commit.Parents...)
	}
	return issue22Reachability{objects: len(objects), files: files}
}

func assertIssue22OwnerIsolation(t *testing.T, environment libraryCLIEnvironment) {
	t.Helper()
	const (
		otherUserID  = "22222222-3333-4444-8555-666666666666"
		missingID    = "99999999-aaaa-4bbb-8ccc-dddddddddddd"
		missingObjID = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	)
	now := time.Now().UTC()
	if err := environment.store.CreateUser(t.Context(), storage.User{ID: otherUserID, Username: "bob", PasswordHash: "unused"}, now); err != nil {
		t.Fatalf("create isolated user: %v", err)
	}
	otherToken := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{8}, 32))
	if err := environment.store.CreateSession(t.Context(), otherUserID, sha256.Sum256([]byte(otherToken)), "issue22", now, now.Add(time.Hour)); err != nil {
		t.Fatalf("create isolated session: %v", err)
	}
	base := mustServerURL(t, environment.server.URL)
	ownerHead, err := getRemoteHead(t.Context(), base, testClientLibraryID, []byte(environment.token))
	if err != nil || ownerHead.CommitID == nil {
		t.Fatalf("read owner Head for isolation: %+v, %v", ownerHead, err)
	}

	for _, path := range []string{"", "/head", "/objects/commits/" + *ownerHead.CommitID} {
		foreignStatus, foreignBody := issue22RawRequest(t, base.JoinPath("v1/libraries", testClientLibraryID).String()+path,
			http.MethodGet, otherToken, nil)
		missingStatus, missingBody := issue22RawRequest(t, base.JoinPath("v1/libraries", missingID).String()+path,
			http.MethodGet, otherToken, nil)
		if foreignStatus != http.StatusNotFound || missingStatus != http.StatusNotFound || !bytes.Equal(foreignBody, missingBody) {
			t.Fatalf("owner isolation path %q differs: foreign=%d/%q missing=%d/%q", path, foreignStatus, foreignBody, missingStatus, missingBody)
		}
	}

	createBody := []byte(`{"Name":"Bob isolated"}`)
	status, _ := issue22RawRequest(t, base.JoinPath("v1/libraries", testClientLibraryID).String(), http.MethodPut, otherToken, createBody)
	if status != http.StatusCreated {
		t.Fatalf("same LibraryId in isolated owner namespace returned %d", status)
	}
	otherHead, err := getRemoteHead(t.Context(), base, testClientLibraryID, []byte(otherToken))
	if err != nil || otherHead.CommitID != nil || otherHead.ETag != `"head-version-0"` {
		t.Fatalf("isolated owner Head=%+v err=%v", otherHead, err)
	}
	for _, id := range []string{*ownerHead.CommitID, missingObjID} {
		status, _ := issue22RawRequest(t, base.JoinPath("v1/libraries", testClientLibraryID, "objects", "commits", id).String(),
			http.MethodGet, otherToken, nil)
		if status != http.StatusNotFound {
			t.Fatalf("isolated object %s returned %d", id, status)
		}
	}
	otherBlock := []byte("bob isolated block")
	otherBlockID := object.ID(otherBlock)
	if err := putBlock(t.Context(), base, testClientLibraryID, []byte(otherToken), otherBlockID, otherBlock); err != nil {
		t.Fatalf("upload other owner block: %v", err)
	}
	status, _ = issue22RawRequest(t, base.JoinPath("v1/libraries", testClientLibraryID, "blocks", otherBlockID).String(),
		http.MethodGet, otherToken, nil)
	if status != http.StatusOK {
		t.Fatalf("other owner block GET returned %d", status)
	}
	status, _ = issue22RawRequest(t, base.JoinPath("v1/libraries", testClientLibraryID, "blocks", otherBlockID).String(),
		http.MethodGet, environment.token, nil)
	if status != http.StatusNotFound {
		t.Fatalf("original owner saw other owner's block: status=%d", status)
	}
	currentOwner, err := getRemoteHead(t.Context(), base, testClientLibraryID, []byte(environment.token))
	if err != nil || currentOwner.CommitID == nil || *currentOwner.CommitID != *ownerHead.CommitID {
		t.Fatalf("other owner changed original Head: before=%+v after=%+v err=%v", ownerHead, currentOwner, err)
	}
	t.Logf("scenario=%q platform=%s filesystem=ext4 owner_head=%s other_head=null isolation=uniform-not-found",
		"two-user isolation", runtime.GOOS, *ownerHead.CommitID)
}

func issue22RawRequest(t *testing.T, target, method, token string, body []byte) (int, []byte) {
	t.Helper()
	request, err := authenticatedRequest(t.Context(), method, target, []byte(token), body)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := noRedirectClient().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatal(errors.Join(readErr, closeErr))
	}
	if response.StatusCode >= http.StatusBadRequest {
		var envelope struct {
			RetCode int
			Message string
		}
		if err := json.Unmarshal(data, &envelope); err != nil || envelope.RetCode == 0 || envelope.Message == "" {
			t.Fatalf("invalid error envelope: status=%d body=%q err=%v", response.StatusCode, data, err)
		}
	}
	return response.StatusCode, data
}
