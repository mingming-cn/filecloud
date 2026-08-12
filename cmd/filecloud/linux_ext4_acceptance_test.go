package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/acceptance"
	libraryapi "github.com/mingming-cn/filecloud/internal/library"
	"github.com/mingming-cn/filecloud/internal/object"
	"github.com/mingming-cn/filecloud/internal/storage"
)

func TestLinuxExt4AcceptanceMatrix(t *testing.T) {
	if os.Getenv("FILECLOUD_LINUX_EXT4_MATRIX_CHILD") == "1" {
		t.Skip("Linux/ext4 acceptance child suite does not recurse")
	}
	if os.Getenv("FILECLOUD_RUN_1A") != "1" {
		t.Skip("set FILECLOUD_RUN_1A=1 to run the Linux/ext4 acceptance matrix")
	}
	ext4Temp, err := os.MkdirTemp(".", ".linux-ext4-matrix-")
	if err != nil {
		t.Fatal(err)
	}
	ext4Temp, err = filepath.Abs(ext4Temp)
	if err != nil {
		t.Fatal(errors.Join(err, os.RemoveAll(ext4Temp)))
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(ext4Temp); err != nil {
			t.Errorf("remove Linux/ext4 matrix directory: %v", err)
		}
	})
	requireLinuxExt4(t, ext4Temp)

	scenarios := []linuxExt4MatrixScenario{
		{category: "correctness oracle", packagePath: "./cmd/filecloud", test: "TestLinuxExt4CorrectnessLoop"},
		{category: "first binding", packagePath: "./cmd/filecloud", test: "TestLibraryBindDoubleEmptyConvergesAndUnbindIsLocalOnly"},
		{category: "first binding", packagePath: "./cmd/filecloud", test: "TestLibraryBindConcurrentInitializationAdoptsWinner"},
		{category: "first binding", packagePath: "./cmd/filecloud", test: "TestLibraryBindImportsLocalSnapshotAndSyncNoOps"},
		{category: "first binding", packagePath: "./cmd/filecloud", test: "TestLibraryBindChecksOutRemoteHeadWithoutMutation"},
		{category: "first binding", packagePath: "./cmd/filecloud", test: "TestLibraryBindCheckoutRejectsBothNonEmptyWithoutMutation"},
		{category: "first binding", packagePath: "./cmd/filecloud", test: "TestLibraryBindRejectsUnsupportedOrNonEmptyAndBindingConflicts"},
		{category: "sync convergence", packagePath: "./cmd/filecloud", test: "TestLibrarySyncStructuralConflictsPreserveCompleteLocalObject"},
		{category: "sync convergence", packagePath: "./cmd/filecloud", test: "TestLibrarySyncMtimeOnlyChangesConvergeWithoutCommitLoop"},
		{category: "sync convergence", packagePath: "./cmd/filecloud", test: "TestLibrarySyncContinuousTrivialMergeCompetition"},
		{category: "sync convergence", packagePath: "./cmd/filecloud", test: "TestLibrarySyncContinuousDivergentHeadConflictsReuseCapturedSeed"},
		{category: "sync convergence", packagePath: "./cmd/filecloud", test: "TestLibrarySyncRecursiveMergeResolvesLostCASResponse"},
		{category: "sync convergence", packagePath: "./cmd/filecloud", test: "TestLibrarySyncRecoversPublishedCandidateBeforeDiscardingMutatedWorktree"},
		{category: "sync convergence", packagePath: "./cmd/filecloud", test: "TestLibrarySyncPendingPublicationTransitionsToSuccessor"},
		{category: "scan races", packagePath: "./cmd/filecloud", test: "TestScanRegularFileRetriesConcurrentRewrite"},
		{category: "scan races", packagePath: "./cmd/filecloud", test: "TestScanDirectoryEnumerationChangeFailsRound"},
		{category: "scan races", packagePath: "./cmd/filecloud", test: "TestScanFinalValidationCatchesEarlierFileChange"},
		{category: "scan races", packagePath: "./cmd/filecloud", test: "TestSyncUnstableScanDoesNotPublishOrChangeClientState"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestFSActionSubprocessCrashMatrix"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestPublicBindSubprocessCrashMatrix"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestPublicInitialCheckoutBaseCommitCrashMatrix"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestPublicSyncSubprocessCrashMatrix"},
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
	runLinuxExt4Matrix(t, ext4Temp, scenarios)
}

type linuxExt4MatrixScenario struct {
	category    string
	packagePath string
	test        string
}

type linuxExt4TestEvent struct {
	Action  string
	Package string
	Test    string
	Output  string
}

type linuxExt4TestKey struct {
	packagePath string
	test        string
}

const _linuxExt4AttestationPrefix = acceptance.Prefix

type linuxExt4Attestation = acceptance.Attestation

func TestLinuxExt4AttestationValidation(t *testing.T) {
	id := strings.Repeat("a", 64)
	root := strings.Repeat("b", 64)
	digest := strings.Repeat("c", 64)
	valid := linuxExt4Attestation{
		Kind: "convergence", Scenario: "stable", Platform: "linux", Filesystem: "ext4",
		Head: id, SyncBase: id, HeadRoot: root, BaseRoot: root, Snapshot: root, ReachableObjects: 3,
		ConfirmedInputDigests: []string{digest}, PreservedInputDigests: []string{digest},
	}
	for _, test := range []struct {
		name         string
		attestations []linuxExt4Attestation
		required     map[string]string
		wantErr      bool
	}{
		{name: "valid convergence", attestations: []linuxExt4Attestation{valid}, required: map[string]string{"stable": "convergence"}},
		{name: "duplicate content is set semantics", attestations: []linuxExt4Attestation{func() linuxExt4Attestation {
			value := valid
			value.ConfirmedInputDigests = []string{digest, digest}
			value.PreservedInputDigests = []string{digest}
			return value
		}()}, required: map[string]string{"stable": "convergence"}},
		{name: "valid empty convergence", attestations: []linuxExt4Attestation{{Kind: "convergence", Scenario: "empty", Platform: "linux", Filesystem: "ext4", Head: id, SyncBase: id, HeadRoot: root, BaseRoot: root, Snapshot: root, ReachableObjects: 2}}, required: map[string]string{"empty": "convergence"}},
		{name: "unknown scenario", attestations: []linuxExt4Attestation{valid}, required: map[string]string{"other": "convergence"}, wantErr: true},
		{name: "duplicate scenario", attestations: []linuxExt4Attestation{valid, valid}, required: map[string]string{"stable": "convergence"}, wantErr: true},
		{name: "snapshot drift", attestations: []linuxExt4Attestation{func() linuxExt4Attestation { value := valid; value.Snapshot = digest; return value }()}, required: map[string]string{"stable": "convergence"}, wantErr: true},
		{name: "missing preserved input", attestations: []linuxExt4Attestation{func() linuxExt4Attestation { value := valid; value.PreservedInputDigests = nil; return value }()}, required: map[string]string{"stable": "convergence"}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateLinuxExt4Attestations(test.attestations, test.required)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateLinuxExt4Attestations() error=%v wantErr=%v", err, test.wantErr)
			}
		})
	}

	if _, found, err := decodeLinuxExt4Attestation("noise"); found || err != nil {
		t.Fatalf("decode noise = found=%v err=%v", found, err)
	}
	line := _linuxExt4AttestationPrefix + `{"kind":"convergence","scenario":"stable","platform":"linux","filesystem":"ext4","unknown":true}`
	if _, found, err := decodeLinuxExt4Attestation(line); !found || err == nil {
		t.Fatalf("decode unknown field = found=%v err=%v", found, err)
	}
	collector := newLinuxExt4AttestationCollector()
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	line = "test.go:1: " + _linuxExt4AttestationPrefix + string(encoded) + "\n"
	middle := len(line) / 2
	if values, err := collector.add("TestStable", line[:middle]); err != nil || len(values) != 0 {
		t.Fatalf("first attestation chunk values=%v err=%v", values, err)
	}
	if values, err := collector.add("TestStable", line[middle:]); err != nil || len(values) != 1 || values[0].Scenario != valid.Scenario {
		t.Fatalf("second attestation chunk values=%v err=%v", values, err)
	}
	if err := collector.close(); err != nil {
		t.Fatal(err)
	}
}

func decodeLinuxExt4Attestation(line string) (linuxExt4Attestation, bool, error) {
	line = strings.TrimSpace(line)
	index := strings.Index(line, _linuxExt4AttestationPrefix)
	if index < 0 {
		return linuxExt4Attestation{}, false, nil
	}
	payload := line[index+len(_linuxExt4AttestationPrefix):]
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.DisallowUnknownFields()
	var result linuxExt4Attestation
	if err := decoder.Decode(&result); err != nil {
		return linuxExt4Attestation{}, true, fmt.Errorf("decode Linux/ext4 attestation: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return linuxExt4Attestation{}, true, errors.New("Linux/ext4 attestation has trailing data")
	}
	return result, true, nil
}

func validateLinuxExt4Attestations(attestations []linuxExt4Attestation, required map[string]string) error {
	seen := make(map[string]bool, len(required))
	for _, attestation := range attestations {
		kind, ok := required[attestation.Scenario]
		if !ok || kind != attestation.Kind {
			return fmt.Errorf("unexpected Linux/ext4 attestation kind=%q scenario=%q", attestation.Kind, attestation.Scenario)
		}
		if seen[attestation.Scenario] {
			return fmt.Errorf("duplicate Linux/ext4 attestation scenario=%q", attestation.Scenario)
		}
		seen[attestation.Scenario] = true
		if attestation.Platform != "linux" || attestation.Filesystem != "ext4" {
			return fmt.Errorf("invalid Linux/ext4 attestation platform=%q filesystem=%q", attestation.Platform, attestation.Filesystem)
		}
		if attestation.UnregisteredInternalPaths != 0 || attestation.ResidualJournalRows != 0 {
			return fmt.Errorf("Linux/ext4 attestation %q has internal paths or journal rows", attestation.Scenario)
		}
		switch attestation.Kind {
		case "convergence":
			if !object.ValidID(attestation.Head) || !object.ValidID(attestation.SyncBase) ||
				!object.ValidID(attestation.HeadRoot) || !object.ValidID(attestation.BaseRoot) ||
				!object.ValidID(attestation.Snapshot) || attestation.Head != attestation.SyncBase ||
				attestation.HeadRoot != attestation.BaseRoot || attestation.HeadRoot != attestation.Snapshot ||
				attestation.ReachableObjects < 2 {
				return fmt.Errorf("Linux/ext4 convergence attestation %q has divergent or invalid state", attestation.Scenario)
			}
			confirmed := uniqueSortedDigests(attestation.ConfirmedInputDigests)
			preserved := uniqueSortedDigests(attestation.PreservedInputDigests)
			if !slices.Equal(confirmed, preserved) ||
				slices.ContainsFunc(confirmed, func(id string) bool { return !object.ValidID(id) }) ||
				slices.ContainsFunc(preserved, func(id string) bool { return !object.ValidID(id) }) {
				return fmt.Errorf("Linux/ext4 convergence attestation %q did not preserve every confirmed input", attestation.Scenario)
			}
		case "isolation":
			if !object.ValidID(attestation.OwnerHead) || attestation.OtherHead != nil || attestation.Isolation != "uniform-not-found" {
				return fmt.Errorf("Linux/ext4 isolation attestation %q is invalid", attestation.Scenario)
			}
		case "server-readability":
			if attestation.FailurePoint == "" || !object.ValidID(attestation.OldHead) ||
				!object.ValidID(attestation.CurrentHead) || attestation.ReachableObjects < 2 {
				return fmt.Errorf("Linux/ext4 server-readability attestation %q is invalid", attestation.Scenario)
			}
		default:
			return fmt.Errorf("unsupported Linux/ext4 attestation kind=%q", attestation.Kind)
		}
	}
	for scenario := range required {
		if !seen[scenario] {
			return fmt.Errorf("missing Linux/ext4 attestation scenario=%q", scenario)
		}
	}
	return nil
}

type linuxExt4AttestationCollector struct {
	pending map[string]*strings.Builder
}

func newLinuxExt4AttestationCollector() *linuxExt4AttestationCollector {
	return &linuxExt4AttestationCollector{pending: make(map[string]*strings.Builder)}
}

func (c *linuxExt4AttestationCollector) add(test, output string) ([]linuxExt4Attestation, error) {
	builder := c.pending[test]
	if builder == nil {
		if !strings.Contains(output, _linuxExt4AttestationPrefix) {
			return nil, nil
		}
		builder = &strings.Builder{}
		c.pending[test] = builder
	}
	builder.WriteString(output)
	if !strings.HasSuffix(output, "\n") {
		return nil, nil
	}
	delete(c.pending, test)
	attestation, found, err := decodeLinuxExt4Attestation(builder.String())
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("completed Linux/ext4 attestation has no marker")
	}
	return []linuxExt4Attestation{attestation}, nil
}

func (c *linuxExt4AttestationCollector) close() error {
	if len(c.pending) != 0 {
		return fmt.Errorf("Linux/ext4 acceptance has %d incomplete attestations", len(c.pending))
	}
	return nil
}

func runLinuxExt4Matrix(t *testing.T, ext4Temp string, scenarios []linuxExt4MatrixScenario) {
	t.Helper()
	command := exec.CommandContext(t.Context(), "go", "test", "-json", "./...", "-count=1", "-timeout=10m")
	command.Dir = filepath.Join("..", "..")
	command.Env = append(os.Environ(),
		"TMPDIR="+ext4Temp,
		"FILECLOUD_RUN_1A=1",
		"FILECLOUD_LINUX_EXT4_ROOT="+ext4Temp,
		"FILECLOUD_LINUX_EXT4_MATRIX_CHILD=1",
	)
	output, runErr := command.CombinedOutput()
	if runErr != nil {
		t.Fatalf("Linux/ext4 full suite: %v\n%s", runErr, output)
	}

	const module = "github.com/mingming-cn/filecloud/"
	required := make(map[linuxExt4TestKey]linuxExt4MatrixScenario, len(scenarios))
	for _, scenario := range scenarios {
		packagePath := module + strings.TrimPrefix(scenario.packagePath, "./")
		required[linuxExt4TestKey{packagePath: packagePath, test: scenario.test}] = scenario
	}
	passed := make(map[linuxExt4TestKey]bool, len(required))
	allowedSkips := map[string]bool{
		"TestHeadUpdateCrashHelper":                      true,
		"TestObjectStorePublicationCrashHelper":          true,
		"TestPublicInitialCheckoutBaseCommitCrashHelper": true,
		"TestPublicSyncTransactionCrashHelper":           true,
		"TestPublicUnbindFSActionCrashHelper":            true,
		"TestPublicSyncFSActionCrashHelper":              true,
		"TestPublicFSActionCrashHelper":                  true,
		"TestFSActionCrashHelper":                        true,
		"TestLinuxExt4AcceptanceMatrix":                  true,
	}
	var attestations []linuxExt4Attestation
	collector := newLinuxExt4AttestationCollector()
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var event linuxExt4TestEvent
		err := decoder.Decode(&event)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode Linux/ext4 full-suite event: %v\n%s", err, output)
		}
		if event.Action == "fail" {
			t.Fatalf("Linux/ext4 test failed: package=%s test=%s\n%s", event.Package, event.Test, output)
		}
		if event.Action == "skip" && !allowedSkips[event.Test] {
			t.Fatalf("Linux/ext4 test skipped: package=%s test=%s\n%s", event.Package, event.Test, output)
		}
		key := linuxExt4TestKey{packagePath: event.Package, test: event.Test}
		if event.Action == "pass" {
			if _, ok := required[key]; ok {
				passed[key] = true
			}
			if event.Test != "" && !strings.Contains(event.Test, "/") {
				t.Logf("package=%q scenario=%q platform=%s filesystem=ext4 result=pass",
					event.Package, event.Test, runtime.GOOS)
			}
		}
		if event.Action == "output" {
			values, err := collector.add(event.Test, event.Output)
			if err != nil {
				t.Fatalf("Linux/ext4 acceptance malformed attestation: %v\n%s", err, output)
			}
			attestations = append(attestations, values...)
		}
	}
	if err := collector.close(); err != nil {
		t.Fatalf("Linux/ext4 acceptance attestations: %v\n%s", err, output)
	}
	for key, scenario := range required {
		if !passed[key] {
			t.Fatalf("Linux/ext4 acceptance required scenario did not pass: category=%q package=%q test=%q",
				scenario.category, key.packagePath, key.test)
		}
	}
	if err := validateLinuxExt4Attestations(attestations, requiredLinuxExt4Attestations()); err != nil {
		t.Fatalf("Linux/ext4 acceptance attestations: %v\n%s", err, output)
	}
	for _, attestation := range attestations {
		line, err := acceptance.Encode(attestation)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(line)
	}
}

func uniqueSortedDigests(values []string) []string {
	result := slices.Clone(values)
	slices.Sort(result)
	return slices.Compact(result)
}

func requiredLinuxExt4Attestations() map[string]string {
	result := map[string]string{
		"publisher import":                             "convergence",
		"subscriber checkout":                          "convergence",
		"independent merge subscriber":                 "convergence",
		"independent merge first client":               "convergence",
		"independent merge second client":              "convergence",
		"conflict preservation first client":           "convergence",
		"conflict preservation second client":          "convergence",
		"two-user isolation":                           "isolation",
		"double-empty binding":                         "convergence",
		"first local import":                           "convergence",
		"first remote checkout":                        "convergence",
		"concurrent initialization first client":       "convergence",
		"concurrent initialization second client":      "convergence",
		"binding conflict original client":             "convergence",
		"binding conflict remote checkout":             "convergence",
		"mtime single-sided":                           "convergence",
		"mtime two-sided first client":                 "convergence",
		"mtime two-sided second client":                "convergence",
		"continuous trivial Head competition":          "convergence",
		"continuous divergent Head competition":        "convergence",
		"recursive lost CAS response":                  "convergence",
		"lost CAS preserves post-publication changes":  "convergence",
		"pending publication became Head ancestor":     "convergence",
		"protected deletion confirmed":                 "convergence",
		"confirmed deletion resumes upload failure":    "convergence",
		"100 MiB upload resumed":                       "convergence",
		"checkout resumes truncated download":          "convergence",
		"checkout rejects wrong digest then resumes":   "convergence",
		"checkout resumes disk failure":                "convergence",
		"initial checkout crash before":                "convergence",
		"initial checkout crash after":                 "convergence",
		"sync transaction crash before_base_commit":    "convergence",
		"sync transaction crash after_base_commit":     "convergence",
		"sync transaction crash before_cleanup_commit": "convergence",
		"sync transaction crash after_cleanup_commit":  "convergence",
		"Head update before":                           "server-readability",
		"Head update after":                            "server-readability",
	}
	for _, scenario := range []string{
		"local delete remote modify file", "local modify remote delete file",
		"local delete remote modify directory", "local modify remote delete directory",
		"local file remote directory", "local directory remote file", "both delete", "identical change",
		"rename is delete plus add",
	} {
		result["structural "+scenario] = "convergence"
	}
	for _, point := range []string{
		"before_temporary_write", "after_temporary_write", "before_temporary_sync", "after_temporary_sync",
		"before_install", "after_install", "before_parent_sync", "after_parent_sync",
	} {
		result["object publication "+point] = "server-readability"
	}
	for _, category := range []struct{ op, kind string }{
		{fsOpCreateFile, "File"}, {fsOpCreateDirectory, "Directory"},
		{fsOpMtime, "File"}, {fsOpMtime, "Directory"},
		{fsOpRename, "File"}, {fsOpRename, "Directory"},
	} {
		for _, point := range []string{"before_intent_commit", "after_intent_commit", "after_action", "after_parent_sync", "after_completed"} {
			result[linuxExt4BindCrashScenario(category.op, category.kind, point)] = "convergence"
		}
	}
	for _, name := range []string{"capture-file", "capture-directory", "post-base-file", "post-base-directory"} {
		for _, point := range []string{"before_intent_commit", "after_intent_commit", "after_action", "after_parent_sync", "after_completed"} {
			result[linuxExt4SyncCrashScenario(name, point)] = "convergence"
		}
	}
	return result
}

func TestLinuxExt4CorrectnessLoop(t *testing.T) {
	if os.Getenv("FILECLOUD_RUN_1A") != "1" {
		t.Skip("set FILECLOUD_RUN_1A=1 to run the Linux/ext4 correctness loop")
	}
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	publisherDir, publisherTree := newLinuxExt4ClientPaths(t)
	subscriberDir, subscriberTree := newLinuxExt4ClientPaths(t)

	confirmed := []linuxExt4ConfirmedInput{
		{name: "base", data: []byte("confirmed base")},
	}
	if err := os.WriteFile(filepath.Join(publisherTree, "shared.txt"), confirmed[0].data, 0o600); err != nil {
		t.Fatal(err)
	}
	bindPublisher := append(bindArgs(publisherDir, environment.server.URL, testClientLibraryID, publisherTree, testClientDeviceID), "--import-local")
	if err := runLinuxExt4CLI(t, bindPublisher, environment.token+"\n"); err != nil {
		t.Fatalf("import publisher: %v", err)
	}
	assertLinuxExt4Converged(t, "publisher import", environment, publisherDir, publisherTree, confirmed)

	if err := runLinuxExt4CLI(t, bindArgs(subscriberDir, environment.server.URL, testClientLibraryID, subscriberTree, testOtherDeviceID), environment.token+"\n"); err != nil {
		t.Fatalf("bind subscriber: %v", err)
	}
	assertLinuxExt4Converged(t, "subscriber checkout", environment, subscriberDir, subscriberTree, confirmed)

	remoteOnly := linuxExt4ConfirmedInput{name: "remote independent change", data: []byte("confirmed remote independent")}
	localOnly := linuxExt4ConfirmedInput{name: "local independent change", data: []byte("confirmed local independent")}
	confirmed = append(confirmed, remoteOnly, localOnly)
	if err := os.WriteFile(filepath.Join(publisherTree, "remote.txt"), remoteOnly.data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runLinuxExt4CLI(t, []string{"library", "sync", "--client-dir", publisherDir, "--worktree", publisherTree}, ""); err != nil {
		t.Fatalf("publish remote independent change: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "local.txt"), localOnly.data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runLinuxExt4CLI(t, []string{"library", "sync", "--client-dir", subscriberDir, "--worktree", subscriberTree}, ""); err != nil {
		t.Fatalf("merge independent changes: %v", err)
	}
	assertLinuxExt4Converged(t, "independent merge subscriber", environment, subscriberDir, subscriberTree, confirmed)
	if err := runLinuxExt4CLI(t, []string{"library", "sync", "--client-dir", publisherDir, "--worktree", publisherTree}, ""); err != nil {
		t.Fatalf("download independent merge: %v", err)
	}
	assertLinuxExt4SameConvergence(t, "independent merge", environment,
		linuxExt4Client{clientDir: publisherDir, worktree: publisherTree},
		linuxExt4Client{clientDir: subscriberDir, worktree: subscriberTree}, confirmed)

	remoteConflict := linuxExt4ConfirmedInput{name: "remote conflicting change", data: []byte("confirmed remote conflict")}
	localConflict := linuxExt4ConfirmedInput{name: "local conflicting change", data: []byte("confirmed local conflict")}
	confirmed = append(confirmed, remoteConflict, localConflict)
	if err := os.WriteFile(filepath.Join(publisherTree, "shared.txt"), remoteConflict.data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runLinuxExt4CLI(t, []string{"library", "sync", "--client-dir", publisherDir, "--worktree", publisherTree}, ""); err != nil {
		t.Fatalf("publish remote conflict: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "shared.txt"), localConflict.data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runLinuxExt4CLI(t, []string{"library", "sync", "--client-dir", subscriberDir, "--worktree", subscriberTree}, ""); err != nil {
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
	if err := runLinuxExt4CLI(t, []string{"library", "sync", "--client-dir", publisherDir, "--worktree", publisherTree}, ""); err != nil {
		t.Fatalf("download conflict result: %v", err)
	}
	assertLinuxExt4SameConvergence(t, "conflict preservation", environment,
		linuxExt4Client{clientDir: publisherDir, worktree: publisherTree},
		linuxExt4Client{clientDir: subscriberDir, worktree: subscriberTree}, confirmed)

	assertLinuxExt4OwnerIsolation(t, environment)
}

type linuxExt4ConfirmedInput struct {
	name   string
	data   []byte
	digest string
}

func (i linuxExt4ConfirmedInput) contentDigest() string {
	if i.digest != "" {
		return i.digest
	}
	return object.ID(i.data)
}

func linuxExt4ConfirmedInputs(values ...string) []linuxExt4ConfirmedInput {
	result := make([]linuxExt4ConfirmedInput, 0, len(values))
	for index, value := range values {
		result = append(result, linuxExt4ConfirmedInput{name: fmt.Sprintf("input-%d", index+1), data: []byte(value)})
	}
	return result
}

func linuxExt4ConfirmedFiles(files map[string][]byte) []linuxExt4ConfirmedInput {
	names := slices.Sorted(maps.Keys(files))
	result := make([]linuxExt4ConfirmedInput, 0, len(names))
	for _, name := range names {
		result = append(result, linuxExt4ConfirmedInput{name: name, data: files[name]})
	}
	return result
}

type linuxExt4Client struct {
	clientDir string
	worktree  string
}

type linuxExt4Reachability struct {
	objects int
	files   map[string]string
}

func runLinuxExt4CLI(t *testing.T, args []string, stdin string) error {
	t.Helper()
	return run(t.Context(), args, bytes.NewBufferString(stdin), io.Discard, io.Discard)
}

func newLinuxExt4ClientPaths(t *testing.T) (string, string) {
	t.Helper()
	clientDir := filepath.Join(t.TempDir(), "client")
	root := os.Getenv("FILECLOUD_LINUX_EXT4_ROOT")
	if root == "" {
		root = "."
	}
	worktree, err := os.MkdirTemp(root, ".linux-ext4-worktree-")
	if err != nil {
		t.Fatalf("create ext4 worktree: %v", err)
	}
	canonical, err := filepath.Abs(worktree)
	if err != nil {
		t.Fatalf("canonicalize ext4 worktree: %v", errors.Join(err, os.RemoveAll(worktree)))
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(canonical); err != nil {
			t.Errorf("remove ext4 worktree: %v", err)
		}
	})
	requireLinuxExt4(t, canonical)
	return clientDir, canonical
}

func requireLinuxExt4(t *testing.T, worktree string) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Fatalf("Linux/ext4 acceptance requires Linux/ext4, got %s", runtime.GOOS)
	}
	root, err := openWorktreeRoot(worktree, requireExt4)
	if err != nil {
		t.Fatalf("Linux/ext4 acceptance requires an ext-family worktree: %v", err)
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

func assertLinuxExt4SameConvergence(t *testing.T, scenario string, environment libraryCLIEnvironment,
	first, second linuxExt4Client, confirmed []linuxExt4ConfirmedInput,
) {
	t.Helper()
	firstBinding := assertLinuxExt4Converged(t, scenario+" first client", environment, first.clientDir, first.worktree, confirmed)
	secondBinding := assertLinuxExt4Converged(t, scenario+" second client", environment, second.clientDir, second.worktree, confirmed)
	if firstBinding.SyncBase != secondBinding.SyncBase || firstBinding.SyncBaseRoot != secondBinding.SyncBaseRoot {
		t.Fatalf("%s clients differ: first=%+v second=%+v", scenario, firstBinding, secondBinding)
	}
}

func assertLinuxExt4Converged(t *testing.T, scenario string, environment libraryCLIEnvironment,
	clientDir, worktree string, confirmed []linuxExt4ConfirmedInput,
) clientBinding {
	t.Helper()
	if os.Getenv("FILECLOUD_RUN_1A") == "1" {
		requireLinuxExt4(t, worktree)
	}
	binding := assertTestConverged(t, environment, clientDir, worktree)
	residualRows := 0
	for _, table := range []string{
		"bind_intents", "pending_publications", "pending_checkouts", "checkout_paths", "sync_recoveries",
		"sync_recovery_promotions", "fs_actions",
	} {
		count := countClientRows(t, clientDir, table, worktree)
		residualRows += count
		if count != 0 {
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

	base := mustServerURL(t, environment.server.URL)
	head, err := getRemoteHead(t.Context(), base, testClientLibraryID, []byte(environment.token))
	if err != nil || head.CommitID == nil {
		t.Fatalf("%s read Head: head=%+v err=%v", scenario, head, err)
	}
	commit, err := getRemoteCommit(t.Context(), base, testClientLibraryID, []byte(environment.token), *head.CommitID)
	if err != nil {
		t.Fatalf("%s read Head Commit: %v", scenario, err)
	}
	root, err := openWorktreeRoot(worktree, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	snapshot, scanErr := scanWorktree(root)
	closeErr := root.Close()
	if scanErr != nil || closeErr != nil {
		t.Fatalf("%s rescan: scan=%v close=%v", scenario, scanErr, closeErr)
	}
	reachable := inspectLinuxExt4Reachability(t, environment, *head.CommitID)
	preserved := make(map[string]bool, len(reachable.files))
	for _, digest := range reachable.files {
		preserved[digest] = true
	}
	confirmedSet := make(map[string]bool, len(confirmed))
	preservedSet := make(map[string]bool, len(confirmed))
	for _, input := range confirmed {
		digest := input.contentDigest()
		confirmedSet[digest] = true
		if preserved[digest] {
			preservedSet[digest] = true
			continue
		}
		t.Fatalf("%s lost confirmed input %q", scenario, input.name)
	}
	confirmedDigests := slices.Sorted(maps.Keys(confirmedSet))
	preservedDigests := slices.Sorted(maps.Keys(preservedSet))
	emitLinuxExt4Attestation(t, linuxExt4Attestation{
		Kind: "convergence", Scenario: scenario, Platform: runtime.GOOS, Filesystem: "ext4",
		Head: *head.CommitID, SyncBase: binding.SyncBase, HeadRoot: commit.Root, BaseRoot: binding.SyncBaseRoot,
		Snapshot: snapshot.root, ReachableObjects: reachable.objects, ConfirmedInputDigests: confirmedDigests,
		PreservedInputDigests: preservedDigests, ResidualJournalRows: residualRows,
	})
	return binding
}

func emitLinuxExt4Attestation(t *testing.T, attestation linuxExt4Attestation) {
	t.Helper()
	if os.Getenv("FILECLOUD_RUN_1A") != "1" {
		return
	}
	line, err := acceptance.Encode(attestation)
	if err != nil {
		t.Fatalf("encode Linux/ext4 attestation: %v", err)
	}
	t.Log(line)
}

func inspectLinuxExt4Reachability(t *testing.T, environment libraryCLIEnvironment, head string) linuxExt4Reachability {
	t.Helper()
	base := mustServerURL(t, environment.server.URL)
	token := []byte(environment.token)
	objects := make(map[string][]byte)
	visited := make(map[string]bool)
	files := make(map[string]string)
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
		visited[key] = true
		if kind != "blocks" {
			objects[key] = data
		}
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
		hash := sha256.New()
		var size int64
		for index, blockID := range file.Blocks {
			block := fetch("blocks", blockID)
			if object.ID(block) != blockID || len(block) == 0 || len(block) > object.MaxBlockSize {
				t.Fatalf("reachable block %s has invalid digest or size %d", blockID, len(block))
			}
			if index < len(file.Blocks)-1 && len(block) != object.MaxBlockSize {
				t.Fatalf("reachable non-tail block %s has size %d", blockID, len(block))
			}
			if _, err := hash.Write(block); err != nil {
				t.Fatal(err)
			}
			size += int64(len(block))
		}
		if size != file.Size {
			t.Fatalf("reachable file %s size=%d want=%d", id, size, file.Size)
		}
		files[id] = fmt.Sprintf("%x", hash.Sum(nil))
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
		if visited["commits\x00"+commitID] {
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
	return linuxExt4Reachability{objects: len(visited), files: files}
}

func assertLinuxExt4OwnerIsolation(t *testing.T, environment libraryCLIEnvironment) {
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
	if err := environment.store.CreateSession(t.Context(), otherUserID, sha256.Sum256([]byte(otherToken)), "linux-ext4-acceptance", now, now.Add(time.Hour)); err != nil {
		t.Fatalf("create isolated session: %v", err)
	}
	base := mustServerURL(t, environment.server.URL)
	ownerHead, err := getRemoteHead(t.Context(), base, testClientLibraryID, []byte(environment.token))
	if err != nil || ownerHead.CommitID == nil {
		t.Fatalf("read owner Head for isolation: %+v, %v", ownerHead, err)
	}

	for _, path := range []string{"", "/head", "/objects/commits/" + *ownerHead.CommitID} {
		foreignStatus, foreignBody := linuxExt4RawRequest(t, base.JoinPath("v1/libraries", testClientLibraryID).String()+path,
			http.MethodGet, otherToken, nil)
		missingStatus, missingBody := linuxExt4RawRequest(t, base.JoinPath("v1/libraries", missingID).String()+path,
			http.MethodGet, otherToken, nil)
		if foreignStatus != http.StatusNotFound || missingStatus != http.StatusNotFound || !bytes.Equal(foreignBody, missingBody) {
			t.Fatalf("owner isolation path %q differs: foreign=%d/%q missing=%d/%q", path, foreignStatus, foreignBody, missingStatus, missingBody)
		}
	}

	createBody := []byte(`{"Name":"Bob isolated"}`)
	status, _ := linuxExt4RawRequest(t, base.JoinPath("v1/libraries", testClientLibraryID).String(), http.MethodPut, otherToken, createBody)
	if status != http.StatusCreated {
		t.Fatalf("same LibraryId in isolated owner namespace returned %d", status)
	}
	otherHead, err := getRemoteHead(t.Context(), base, testClientLibraryID, []byte(otherToken))
	if err != nil || otherHead.CommitID != nil || otherHead.ETag != `"head-version-0"` {
		t.Fatalf("isolated owner Head=%+v err=%v", otherHead, err)
	}
	for _, id := range []string{*ownerHead.CommitID, missingObjID} {
		status, _ := linuxExt4RawRequest(t, base.JoinPath("v1/libraries", testClientLibraryID, "objects", "commits", id).String(),
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
	status, _ = linuxExt4RawRequest(t, base.JoinPath("v1/libraries", testClientLibraryID, "blocks", otherBlockID).String(),
		http.MethodGet, otherToken, nil)
	if status != http.StatusOK {
		t.Fatalf("other owner block GET returned %d", status)
	}
	status, _ = linuxExt4RawRequest(t, base.JoinPath("v1/libraries", testClientLibraryID, "blocks", otherBlockID).String(),
		http.MethodGet, environment.token, nil)
	if status != http.StatusNotFound {
		t.Fatalf("original owner saw other owner's block: status=%d", status)
	}
	currentOwner, err := getRemoteHead(t.Context(), base, testClientLibraryID, []byte(environment.token))
	if err != nil || currentOwner.CommitID == nil || *currentOwner.CommitID != *ownerHead.CommitID {
		t.Fatalf("other owner changed original Head: before=%+v after=%+v err=%v", ownerHead, currentOwner, err)
	}
	emitLinuxExt4Attestation(t, linuxExt4Attestation{
		Kind: "isolation", Scenario: "two-user isolation", Platform: runtime.GOOS, Filesystem: "ext4",
		OwnerHead: *ownerHead.CommitID, Isolation: "uniform-not-found",
	})
}

func linuxExt4RawRequest(t *testing.T, target, method, token string, body []byte) (int, []byte) {
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
