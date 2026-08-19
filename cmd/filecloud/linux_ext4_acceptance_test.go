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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/acceptance"
	"github.com/mingming-cn/filecloud/internal/fscompat"
	libraryapi "github.com/mingming-cn/filecloud/internal/library"
	"github.com/mingming-cn/filecloud/internal/object"
	"github.com/mingming-cn/filecloud/internal/storage"
)

func platformMatrixScenarios() []platformMatrixScenario {
	return []platformMatrixScenario{
		{category: "fixed vectors", packagePath: "./internal/object", test: "TestCanonicalMetadataVectors"},
		{category: "fixed vectors", packagePath: "./internal/object", test: "TestBlockVectors"},
		{category: "fixed vectors", packagePath: "./cmd/filecloud", test: "TestCanonicalProtocolObjectIDs"},
		{category: "fixed vectors", packagePath: "./cmd/filecloud", test: "TestConflictCopyNameAttestedNormalVectors"},
		{category: "fixed vectors", packagePath: "./cmd/filecloud", test: "TestConflictCopyNameLongExtensionOrdinalOneFixedVector"},
		{category: "fixed vectors", packagePath: "./cmd/filecloud", test: "TestConflictFallbackAttestedVectors"},
		{category: "fixed vectors", packagePath: "./cmd/filecloud", test: "TestLibrarySyncLongFallbackCandidateHistoryFixedReplayVector"},
		{category: "fixed vectors", packagePath: "./cmd/filecloud", test: "TestCrossPlatformFixedObjectIDs"},
		{category: "fixed convergence", packagePath: "./cmd/filecloud", test: "TestCrossPlatformDeterministicConvergence"},
		{category: "correctness oracle", packagePath: "./cmd/filecloud", test: "TestPlatformCorrectnessLoop"},
		{category: "real process acceptance", packagePath: "./cmd/filecloud", test: "TestBuiltBinaryEndToEndAcceptance"},
		{category: "real process acceptance", packagePath: "./cmd/filecloud", test: "TestBuiltBinaryHistoryInspectReadOnlyAcceptance"},
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
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestPublicCreateZeroIdentityReplacementPreserved"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestPublicDeepCreateZeroIdentityPreserved"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestPublicCreateZeroIdentityTypeChangedEntryPreserved"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestPublicCreateRecoveryCollisionChain"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestPublicSyncZeroIdentityCreateAutoRollback"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestPublicSyncZeroIdentityTypeChangedEntryPreserved"},
		{category: "checkout fault injection", packagePath: "./cmd/filecloud", test: "TestPublicSyncCreateRecoveryCollisionChain"},
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
}

type platformMatrixScenario struct {
	category    string
	packagePath string
	test        string
}

func TestPlatformMatrixWiresFullScenarioSet(t *testing.T) {
	required := make(map[string]bool)
	for _, scenario := range platformMatrixScenarios() {
		required[scenario.test] = true
	}
	for _, test := range []string{
		"TestCanonicalMetadataVectors",
		"TestBlockVectors",
		"TestConflictFallbackAttestedVectors",
		"TestLibrarySyncLongFallbackCandidateHistoryFixedReplayVector",
		"TestCrossPlatformFixedObjectIDs",
		"TestCrossPlatformDeterministicConvergence",
		"TestPlatformCorrectnessLoop",
		"TestBuiltBinaryEndToEndAcceptance",
		"TestBuiltBinaryHistoryInspectReadOnlyAcceptance",
		"TestLibraryBindDoubleEmptyConvergesAndUnbindIsLocalOnly",
		"TestScanRegularFileRetriesConcurrentRewrite",
		"TestFSActionSubprocessCrashMatrix",
		"TestPublicBindSubprocessCrashMatrix",
		"TestPublicSyncSubprocessCrashMatrix",
		"TestLibrarySyncInterrupted100MiBUploadSendsOnlyMissingBlocks",
		"TestLibrarySyncStructuralConflictsPreserveCompleteLocalObject",
	} {
		if !required[test] {
			t.Fatalf("cross-platform matrix omitted %s", test)
		}
	}
}

type platformTestEvent struct {
	Action  string
	Package string
	Test    string
	Output  string
}

func platformSkipAllowed(event platformTestEvent, allowed map[platformTestKey]bool) bool {
	return event.Test == "" || allowed[platformTestKey{packagePath: event.Package, test: event.Test}]
}

func TestPlatformSkipAllowed(t *testing.T) {
	const module = "example/"
	allowed, _ := platformMatrixAllowedSkips(module, "linux")
	for _, test := range []struct {
		name  string
		event platformTestEvent
		want  bool
	}{
		{name: "package without test files", event: platformTestEvent{Action: "skip", Package: module + "no-tests"}, want: true},
		{name: "registered helper", event: platformTestEvent{Action: "skip", Package: module + "internal/library", Test: "TestHeadUpdateCrashHelper"}, want: true},
		{name: "1C command performance test", event: platformTestEvent{Action: "skip", Package: module + "cmd/filecloud", Test: "TestPerformanceBaselineLargeFile"}, want: true},
		{name: "1C object performance test", event: platformTestEvent{Action: "skip", Package: module + "internal/object", Test: "TestPerformanceBaselineWideDirectory"}, want: true},
		{name: "same name from another package", event: platformTestEvent{Action: "skip", Package: module + "other", Test: "TestHeadUpdateCrashHelper"}, want: false},
		{name: "unknown performance test", event: platformTestEvent{Action: "skip", Package: module + "internal/object", Test: "TestPerformanceBaselineUnexpected"}, want: false},
		{name: "unknown named test", event: platformTestEvent{Action: "skip", Package: module + "internal/library", Test: "TestUnexpected"}, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := platformSkipAllowed(test.event, allowed); got != test.want {
				t.Fatalf("platformSkipAllowed()=%t, want %t", got, test.want)
			}
		})
	}
}

type platformTestKey struct {
	packagePath string
	test        string
}

const _platformAttestationPrefix = acceptance.Prefix

type platformAttestation = acceptance.Attestation

func TestPlatformAttestationValidation(t *testing.T) {
	id := strings.Repeat("a", 64)
	root := strings.Repeat("b", 64)
	digest := strings.Repeat("c", 64)
	valid := platformAttestation{
		Kind: "convergence", Scenario: "stable", Platform: "linux", Filesystem: "ext4",
		Head: id, SyncBase: id, HeadRoot: root, BaseRoot: root, Snapshot: root, ReachableObjects: 3,
		ConfirmedInputDigests: []string{digest}, PreservedInputDigests: []string{digest},
	}
	recovery := platformAttestation{
		Kind: "recovery", Scenario: "recover", Platform: "linux", Filesystem: "ext4",
		FailurePoint: "between_create_identity", OldHead: id, CurrentHead: id,
		ConfirmedInputDigests: []string{digest}, PreservedInputDigests: []string{digest},
	}
	primitives := platformAttestation{
		Kind: "filesystem-primitives", Scenario: "filesystem primitives", Platform: "darwin", Filesystem: "apfs",
		NoFollow: true, StableFileIdentity: true, NoReplaceRename: true, NoReplaceLink: true,
		CasefoldLookup: "case-insensitive-alias", SameDirectoryRename: true, DirectorySync: true,
		CrossProcessLock: true, OldFDWritesDetached: true, Warning: "old fd boundary",
	}
	vectors := platformAttestation{
		Kind: "fixed-vectors", Scenario: "cross-platform fixed vectors", Platform: "linux", Filesystem: "ext4",
		VectorIDs: expectedCrossPlatformVectorIDs(),
	}
	for _, test := range []struct {
		name                 string
		attestations         []platformAttestation
		required             map[string]string
		platform, filesystem string
		wantErr              bool
	}{
		{name: "valid convergence", attestations: []platformAttestation{valid}, required: map[string]string{"stable": "convergence"}},
		{name: "duplicate content is set semantics", attestations: []platformAttestation{func() platformAttestation {
			value := valid
			value.ConfirmedInputDigests = []string{digest, digest}
			value.PreservedInputDigests = []string{digest}
			return value
		}()}, required: map[string]string{"stable": "convergence"}},
		{name: "valid empty convergence", attestations: []platformAttestation{{Kind: "convergence", Scenario: "empty", Platform: "linux", Filesystem: "ext4", Head: id, SyncBase: id, HeadRoot: root, BaseRoot: root, Snapshot: root, ReachableObjects: 2}}, required: map[string]string{"empty": "convergence"}},
		{name: "valid recovery", attestations: []platformAttestation{recovery}, required: map[string]string{"recover": "recovery"}},
		{name: "valid APFS primitives", attestations: []platformAttestation{primitives}, required: map[string]string{"filesystem primitives": "filesystem-primitives"}, platform: "darwin", filesystem: "apfs"},
		{name: "unknown APFS casefold lookup", attestations: []platformAttestation{func() platformAttestation { value := primitives; value.CasefoldLookup = "unknown"; return value }()}, required: map[string]string{"filesystem primitives": "filesystem-primitives"}, platform: "darwin", filesystem: "apfs", wantErr: true},
		{name: "valid fixed vectors", attestations: []platformAttestation{vectors}, required: map[string]string{"cross-platform fixed vectors": "fixed-vectors"}},
		{name: "fixed vector drift", attestations: []platformAttestation{func() platformAttestation {
			value := vectors
			value.VectorIDs = maps.Clone(value.VectorIDs)
			value.VectorIDs["merge/directory"] = root
			return value
		}()}, required: map[string]string{"cross-platform fixed vectors": "fixed-vectors"}, wantErr: true},
		{name: "recovery Head drift", attestations: []platformAttestation{func() platformAttestation { value := recovery; value.CurrentHead = root; return value }()}, required: map[string]string{"recover": "recovery"}, wantErr: true},
		{name: "recovery lost input", attestations: []platformAttestation{func() platformAttestation { value := recovery; value.PreservedInputDigests = nil; return value }()}, required: map[string]string{"recover": "recovery"}, wantErr: true},
		{name: "unknown scenario", attestations: []platformAttestation{valid}, required: map[string]string{"other": "convergence"}, wantErr: true},
		{name: "duplicate scenario", attestations: []platformAttestation{valid, valid}, required: map[string]string{"stable": "convergence"}, wantErr: true},
		{name: "snapshot drift", attestations: []platformAttestation{func() platformAttestation { value := valid; value.Snapshot = digest; return value }()}, required: map[string]string{"stable": "convergence"}, wantErr: true},
		{name: "missing preserved input", attestations: []platformAttestation{func() platformAttestation { value := valid; value.PreservedInputDigests = nil; return value }()}, required: map[string]string{"stable": "convergence"}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			platform, filesystem := test.platform, test.filesystem
			if platform == "" {
				platform, filesystem = "linux", "ext4"
			}
			err := validatePlatformAttestations(test.attestations, test.required, platform, filesystem)
			if (err != nil) != test.wantErr {
				t.Fatalf("validatePlatformAttestations() error=%v wantErr=%v", err, test.wantErr)
			}
		})
	}

	if _, found, err := decodePlatformAttestation("noise"); found || err != nil {
		t.Fatalf("decode noise = found=%v err=%v", found, err)
	}
	line := _platformAttestationPrefix + `{"kind":"convergence","scenario":"stable","platform":"linux","filesystem":"ext4","unknown":true}`
	if _, found, err := decodePlatformAttestation(line); !found || err == nil {
		t.Fatalf("decode unknown field = found=%v err=%v", found, err)
	}
	collector := newPlatformAttestationCollector()
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	line = "test.go:1: " + _platformAttestationPrefix + string(encoded) + "\n"
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

func decodePlatformAttestation(line string) (platformAttestation, bool, error) {
	line = strings.TrimSpace(line)
	index := strings.Index(line, _platformAttestationPrefix)
	if index < 0 {
		return platformAttestation{}, false, nil
	}
	payload := line[index+len(_platformAttestationPrefix):]
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.DisallowUnknownFields()
	var result platformAttestation
	if err := decoder.Decode(&result); err != nil {
		return platformAttestation{}, true, fmt.Errorf("decode platform attestation: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return platformAttestation{}, true, errors.New("platform attestation has trailing data")
	}
	return result, true, nil
}

func validatePlatformAttestations(attestations []platformAttestation, required map[string]string, platform, filesystem string) error {
	seen := make(map[string]bool, len(required))
	for _, attestation := range attestations {
		kind, ok := required[attestation.Scenario]
		if !ok || kind != attestation.Kind {
			return fmt.Errorf("unexpected platform attestation kind=%q scenario=%q", attestation.Kind, attestation.Scenario)
		}
		if seen[attestation.Scenario] {
			return fmt.Errorf("duplicate platform attestation scenario=%q", attestation.Scenario)
		}
		seen[attestation.Scenario] = true
		if attestation.Platform != platform || attestation.Filesystem != filesystem {
			return fmt.Errorf("invalid platform attestation platform=%q filesystem=%q, want %q/%q",
				attestation.Platform, attestation.Filesystem, platform, filesystem)
		}
		if attestation.UnregisteredInternalPaths != 0 || attestation.ResidualJournalRows != 0 {
			return fmt.Errorf("platform attestation %q has internal paths or journal rows", attestation.Scenario)
		}
		switch attestation.Kind {
		case "convergence":
			if !object.ValidID(attestation.Head) || !object.ValidID(attestation.SyncBase) ||
				!object.ValidID(attestation.HeadRoot) || !object.ValidID(attestation.BaseRoot) ||
				!object.ValidID(attestation.Snapshot) || attestation.Head != attestation.SyncBase ||
				attestation.HeadRoot != attestation.BaseRoot || attestation.HeadRoot != attestation.Snapshot ||
				attestation.ReachableObjects < 2 {
				return fmt.Errorf("platform convergence attestation %q has divergent or invalid state", attestation.Scenario)
			}
			confirmed := uniqueSortedDigests(attestation.ConfirmedInputDigests)
			preserved := uniqueSortedDigests(attestation.PreservedInputDigests)
			if !slices.Equal(confirmed, preserved) ||
				slices.ContainsFunc(confirmed, func(id string) bool { return !object.ValidID(id) }) ||
				slices.ContainsFunc(preserved, func(id string) bool { return !object.ValidID(id) }) {
				return fmt.Errorf("platform convergence attestation %q did not preserve every confirmed input", attestation.Scenario)
			}
		case "isolation":
			if !object.ValidID(attestation.OwnerHead) || attestation.OtherHead != nil || attestation.Isolation != "uniform-not-found" {
				return fmt.Errorf("platform isolation attestation %q is invalid", attestation.Scenario)
			}
		case "server-readability":
			if attestation.FailurePoint == "" || !object.ValidID(attestation.OldHead) ||
				!object.ValidID(attestation.CurrentHead) || attestation.ReachableObjects < 2 {
				return fmt.Errorf("platform server-readability attestation %q is invalid", attestation.Scenario)
			}
		case "recovery":
			confirmed := uniqueSortedDigests(attestation.ConfirmedInputDigests)
			preserved := uniqueSortedDigests(attestation.PreservedInputDigests)
			validBase := attestation.PreviousSyncBase == "" && attestation.SyncBase == "" ||
				attestation.PreviousSyncBase == attestation.SyncBase && object.ValidID(attestation.SyncBase)
			if attestation.FailurePoint != "between_create_identity" || !object.ValidID(attestation.OldHead) ||
				attestation.OldHead != attestation.CurrentHead || !validBase || len(confirmed) == 0 ||
				!slices.Equal(confirmed, preserved) ||
				slices.ContainsFunc(confirmed, func(id string) bool { return !object.ValidID(id) }) {
				return fmt.Errorf("platform recovery attestation %q is invalid", attestation.Scenario)
			}
		case "fixed-vectors":
			if !maps.Equal(attestation.VectorIDs, expectedCrossPlatformVectorIDs()) {
				return fmt.Errorf("platform fixed-vectors attestation %q differs from the cross-platform contract", attestation.Scenario)
			}
		case "filesystem-primitives":
			validCasefold := attestation.CasefoldLookup == "case-insensitive-alias" ||
				attestation.CasefoldLookup == "case-sensitive-distinct"
			if !attestation.NoFollow || !attestation.StableFileIdentity || !attestation.NoReplaceRename || !attestation.NoReplaceLink ||
				!validCasefold || !attestation.SameDirectoryRename || !attestation.DirectorySync || !attestation.CrossProcessLock {
				return fmt.Errorf("platform filesystem-primitives attestation %q is incomplete", attestation.Scenario)
			}
			if platform == "windows" && filesystem == "ntfs" {
				if !attestation.OccupiedRenamePreserved {
					return fmt.Errorf("Windows filesystem-primitives attestation %q is incomplete", attestation.Scenario)
				}
				break
			}
			if !attestation.OldFDWritesDetached || attestation.Warning == "" {
				return fmt.Errorf("platform filesystem-primitives attestation %q is incomplete", attestation.Scenario)
			}
		default:
			return fmt.Errorf("unsupported platform attestation kind=%q", attestation.Kind)
		}
	}
	for scenario := range required {
		if !seen[scenario] {
			return fmt.Errorf("missing platform attestation scenario=%q", scenario)
		}
	}
	return nil
}

type platformAttestationCollector struct {
	pending map[string]*strings.Builder
}

func newPlatformAttestationCollector() *platformAttestationCollector {
	return &platformAttestationCollector{pending: make(map[string]*strings.Builder)}
}

func (c *platformAttestationCollector) add(test, output string) ([]platformAttestation, error) {
	builder := c.pending[test]
	if builder == nil {
		if !strings.Contains(output, _platformAttestationPrefix) {
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
	attestation, found, err := decodePlatformAttestation(builder.String())
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("completed platform attestation has no marker")
	}
	return []platformAttestation{attestation}, nil
}

func (c *platformAttestationCollector) close() error {
	if len(c.pending) != 0 {
		return fmt.Errorf("platform acceptance has %d incomplete attestations", len(c.pending))
	}
	return nil
}

func platformMatrixEnvironment() []string {
	return []string{
		"FILECLOUD_RUN_1B=1",
		"FILECLOUD_RUN_1A=",
		"FILECLOUD_RUN_1B_APFS=",
		"FILECLOUD_RUN_1B_NTFS=",
		"FILECLOUD_RUN_1C=",
	}
}

func TestPlatformMatrixEnvironmentDisables1C(t *testing.T) {
	if !slices.Contains(platformMatrixEnvironment(), "FILECLOUD_RUN_1C=") {
		t.Fatal("platform matrix environment does not disable FILECLOUD_RUN_1C")
	}
}

func platformMatrixAllowedSkips(module, platform string) (map[platformTestKey]bool, map[string]bool) {
	commandPackage := module + "cmd/filecloud"
	allowed := map[platformTestKey]bool{
		{packagePath: module + "internal/library", test: "TestHeadUpdateCrashHelper"}:             true,
		{packagePath: module + "internal/object", test: "TestPerformanceBaselineWideDirectory"}:   true,
		{packagePath: module + "internal/storage", test: "TestGarbageCollectorCrashHelper"}:       true,
		{packagePath: module + "internal/storage", test: "TestObjectStorePublicationCrashHelper"}: true,
		{packagePath: commandPackage, test: "TestPerformanceBaselineSmallFiles"}:                  true,
		{packagePath: commandPackage, test: "TestPerformanceBaselineLargeFile"}:                   true,
		{packagePath: commandPackage, test: "TestPerformanceBaselineKDF"}:                         true,
		{packagePath: commandPackage, test: "TestPublicInitialCheckoutBaseCommitCrashHelper"}:     true,
		{packagePath: commandPackage, test: "TestPublicSyncTransactionCrashHelper"}:               true,
		{packagePath: commandPackage, test: "TestPublicUnbindFSActionCrashHelper"}:                true,
		{packagePath: commandPackage, test: "TestPublicSyncFSActionCrashHelper"}:                  true,
		{packagePath: commandPackage, test: "TestPublicFSActionCrashHelper"}:                      true,
		{packagePath: commandPackage, test: "TestFSActionCrashHelper"}:                            true,
		{packagePath: commandPackage, test: "TestCrossPlatformAcceptanceMatrix"}:                  true,
		{packagePath: commandPackage, test: "TestLinuxExt4LockHelper"}:                            true,
		{packagePath: commandPackage, test: "TestMacOSAPFSLockHelper"}:                            true,
		{packagePath: commandPackage, test: "TestWindowsNTFSLockHelper"}:                          true,
	}
	casefold := make(map[string]bool)
	if platform != "darwin" && platform != "windows" {
		return allowed, casefold
	}
	for _, test := range []string{
		"TestLibraryBindImportRejectsUnsupportedAndCollidingPaths/casefold",
		"TestLibrarySyncLongConflictNamesConverge/root_fallback_casefold_alias",
		"TestLibrarySyncCasefoldAliasRaceRelocatesCapturedPromotion",
		"TestLibrarySyncCasefoldAliasRaceKeepsFixedCapturedTarget",
		"TestLibrarySyncCasefoldAliasRelocationCrashMatrix",
	} {
		allowed[platformTestKey{packagePath: commandPackage, test: test}] = true
		casefold[test] = true
	}
	return allowed, casefold
}

func runPlatformMatrix(t *testing.T, filesystemRoot string, scenarios []platformMatrixScenario,
	platform, filesystem string, environment []string,
) {
	t.Helper()
	command := exec.CommandContext(t.Context(), "go", "test", "-json", "./...", "-count=1", "-timeout=20m")
	command.Dir = filepath.Join("..", "..")
	command.Env = append(os.Environ(),
		append([]string{
			"TMPDIR=" + filesystemRoot,
			"TMP=" + filesystemRoot,
			"TEMP=" + filesystemRoot,
			"FILECLOUD_ACCEPTANCE_ROOT=" + filesystemRoot,
			"FILECLOUD_PLATFORM_MATRIX_CHILD=1",
		}, environment...)...,
	)
	output, runErr := command.CombinedOutput()
	if runErr != nil {
		t.Fatalf("platform full suite: %v\n%s", runErr, output)
	}

	const module = "github.com/mingming-cn/filecloud/"
	required := make(map[platformTestKey]platformMatrixScenario, len(scenarios))
	for _, scenario := range scenarios {
		packagePath := module + strings.TrimPrefix(scenario.packagePath, "./")
		required[platformTestKey{packagePath: packagePath, test: scenario.test}] = scenario
	}
	passed := make(map[platformTestKey]bool, len(required))
	allowedSkips, casefoldSkips := platformMatrixAllowedSkips(module, platform)
	var attestations []platformAttestation
	actualCasefoldSkips := make(map[string]bool)
	collector := newPlatformAttestationCollector()
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var event platformTestEvent
		err := decoder.Decode(&event)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode platform full-suite event: %v\n%s", err, output)
		}
		if event.Action == "fail" {
			t.Fatalf("platform test failed: package=%s test=%s\n%s", event.Package, event.Test, output)
		}
		if event.Action == "skip" {
			if !platformSkipAllowed(event, allowedSkips) {
				t.Fatalf("platform test skipped: package=%s test=%s\n%s", event.Package, event.Test, output)
			}
			if casefoldSkips[event.Test] {
				actualCasefoldSkips[event.Test] = true
			}
		}
		key := platformTestKey{packagePath: event.Package, test: event.Test}
		if event.Action == "pass" {
			if _, ok := required[key]; ok {
				passed[key] = true
			}
			if event.Test != "" && !strings.Contains(event.Test, "/") {
				t.Logf("package=%q scenario=%q platform=%s filesystem=%s result=pass",
					event.Package, event.Test, platform, filesystem)
			}
		}
		if event.Action == "output" {
			values, err := collector.add(event.Test, event.Output)
			if err != nil {
				t.Fatalf("platform acceptance malformed attestation: %v\n%s", err, output)
			}
			attestations = append(attestations, values...)
		}
	}
	if err := collector.close(); err != nil {
		t.Fatalf("platform acceptance attestations: %v\n%s", err, output)
	}
	for key, scenario := range required {
		if !passed[key] {
			t.Fatalf("platform acceptance required scenario did not pass: category=%q package=%q test=%q",
				scenario.category, key.packagePath, key.test)
		}
	}
	if err := validatePlatformAttestations(attestations, requiredPlatformAttestations(platform, filesystem), platform, filesystem); err != nil {
		t.Fatalf("platform acceptance attestations: %v\n%s", err, output)
	}
	if len(actualCasefoldSkips) != 0 {
		caseInsensitiveProof := false
		for _, attestation := range attestations {
			caseInsensitiveProof = caseInsensitiveProof ||
				attestation.Kind == "filesystem-primitives" && attestation.CasefoldLookup == "case-insensitive-alias"
		}
		if len(actualCasefoldSkips) != len(casefoldSkips) || !caseInsensitiveProof {
			t.Fatalf("platform casefold skips=%v proof=%t, want all %d skips with case-insensitive proof",
				actualCasefoldSkips, caseInsensitiveProof, len(casefoldSkips))
		}
	}
	for _, attestation := range attestations {
		line, err := acceptance.Encode(attestation)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(line)
	}
}

func TestCrossPlatformFixedObjectIDs(t *testing.T) {
	got, err := computeCrossPlatformVectorIDs()
	if err != nil {
		t.Fatal(err)
	}
	want := expectedCrossPlatformVectorIDs()
	if !maps.Equal(got, want) {
		t.Fatalf("cross-platform vector IDs=%v, want %v", got, want)
	}
	platform, filesystem, _ := acceptance.ActivePlatform()
	emitPlatformAttestation(t, platformAttestation{
		Kind: "fixed-vectors", Scenario: "cross-platform fixed vectors", Platform: platform, Filesystem: filesystem,
		VectorIDs: got,
	})
}

func computeCrossPlatformVectorIDs() (map[string]string, error) {
	result := make(map[string]string, 7)
	result["block/one-byte"] = object.ID([]byte("a"))

	_, emptyFileID, err := object.Canonicalize("files", []byte(`{"Version":1,"Size":"0","Blocks":[],"Type":"File"}`))
	if err != nil {
		return nil, err
	}
	result["object/empty-file"] = emptyFileID
	_, unicodeDirectoryID, err := object.Canonicalize("directories", []byte(`{"Entries":[{"Type":"File","Name":"Å.txt","ModifiedAt":"2026-08-09T00:00:00Z","Id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}],"Type":"Directory","Version":1}`))
	if err != nil {
		return nil, err
	}
	result["object/unicode-directory"] = unicodeDirectoryID

	_, mtimeDirectoryID, err := canonicalDirectory("", []scanEntry{{name: "fixed.txt", kind: "File",
		id: strings.Repeat("a", 64), modified: canonicalProtocolMtime(time.Date(2026, 2, 3, 12, 5, 6, 900_000_000,
			time.FixedZone("+8", 8*60*60)))}})
	if err != nil {
		return nil, err
	}
	result["mtime/directory"] = mtimeDirectoryID
	_, mtimeCommitID, err := canonicalCommit(testClientUserID, testClientDeviceID, mtimeDirectoryID,
		[]string{strings.Repeat("b", 64)}, func() time.Time {
			return time.Date(2026, 2, 3, 4, 5, 7, 800_000_000, time.UTC)
		})
	if err != nil {
		return nil, err
	}
	result["mtime/commit"] = mtimeCommitID

	seed := object.Commit{AuthorUserID: testClientUserID, DeviceID: testClientDeviceID, CreatedAt: "2025-02-03T04:05:06Z"}
	conflictName, err := _conflictCopyName(strings.Repeat("界", 80)+".txt", "", seed, nil)
	if err != nil {
		return nil, err
	}
	_, conflictDirectoryID, err := canonicalDirectory("", []scanEntry{{name: conflictName, kind: "File",
		id: strings.Repeat("c", 64), modified: "2026-02-03T04:05:06Z"}})
	if err != nil {
		return nil, err
	}
	result["conflict/directory"] = conflictDirectoryID

	emptyData, emptyID, err := canonicalEmptyDirectory()
	if err != nil {
		return nil, err
	}
	localData, localID, err := canonicalDirectory("", []scanEntry{{name: "local.txt", kind: "File",
		id: strings.Repeat("d", 64), modified: "2026-02-03T04:05:06Z"}})
	if err != nil {
		return nil, err
	}
	remoteData, remoteID, err := canonicalDirectory("", []scanEntry{{name: "remote.txt", kind: "File",
		id: strings.Repeat("e", 64), modified: "2026-02-03T04:05:07Z"}})
	if err != nil {
		return nil, err
	}
	merger := &_treeMerger{
		directories: map[string][]byte{emptyID: emptyData, localID: localData, remoteID: remoteData},
		synthesized: make(map[string][]byte), active: make(map[string]bool), seen: make(map[string]bool),
		budget: _newReplayBudget(), localSeed: seed, localSeedID: strings.Repeat("f", 64),
		lineage: make(map[string]_conflictPromotion),
	}
	mergedID, err := merger.merge(emptyID, localID, remoteID, "", 0)
	if err != nil {
		return nil, err
	}
	result["merge/directory"] = mergedID
	return result, nil
}

func expectedCrossPlatformVectorIDs() map[string]string {
	return map[string]string{
		"block/one-byte":           "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
		"object/empty-file":        "fe680f5ed33eb93ec5fb2eba2003164fe1d60401cc74edd895042aeb17220032",
		"object/unicode-directory": "ed45fcac78692b7aef42e4a4cdd131b2fca7bde0f02b80c3fa6d5eaf5da27e92",
		"mtime/directory":          "b53e9e0a56bec96806fb3df7c03b9ac609514a640f4959ff5e1d40b210049b39",
		"mtime/commit":             "9fdc5eae97573252943f980fdeea40022078064e3c977d2d194561b8fbb0002b",
		"conflict/directory":       "4c4f5c0368f699b0b1c2dcc5bcf8245f5850bba8e68a85038272ef1482baa84c",
		"merge/directory":          "46fd9a1f6679629303c712ce883d20252ea078e6f7983ebeb1afdaab340f8ce8",
	}
}

func TestRequiredPlatformAttestationCounts(t *testing.T) {
	for _, test := range []struct {
		platform   string
		filesystem string
	}{
		{platform: "linux", filesystem: "ext4"},
		{platform: "darwin", filesystem: "apfs"},
		{platform: "windows", filesystem: "ntfs"},
	} {
		if got := len(requiredPlatformAttestations(test.platform, test.filesystem)); got != 113 {
			t.Fatalf("%s/%s attestations=%d, want 113", test.platform, test.filesystem, got)
		}
	}
}

func uniqueSortedDigests(values []string) []string {
	result := slices.Clone(values)
	slices.Sort(result)
	return slices.Compact(result)
}

func requiredPlatformAttestations(platform, filesystem string) map[string]string {
	result := map[string]string{
		"real binary CLI lifecycle":                       "convergence",
		"publisher import":                                "convergence",
		"subscriber checkout":                             "convergence",
		"independent merge subscriber":                    "convergence",
		"independent merge first client":                  "convergence",
		"independent merge second client":                 "convergence",
		"conflict preservation first client":              "convergence",
		"conflict preservation second client":             "convergence",
		"two-user isolation":                              "isolation",
		"double-empty binding":                            "convergence",
		"first local import":                              "convergence",
		"first remote checkout":                           "convergence",
		"concurrent initialization first client":          "convergence",
		"concurrent initialization second client":         "convergence",
		"binding conflict original client":                "convergence",
		"binding conflict remote checkout":                "convergence",
		"mtime single-sided":                              "convergence",
		"mtime two-sided first client":                    "convergence",
		"mtime two-sided second client":                   "convergence",
		"continuous trivial Head competition":             "convergence",
		"continuous divergent Head competition":           "convergence",
		"recursive lost CAS response":                     "convergence",
		"lost CAS preserves post-publication changes":     "convergence",
		"pending publication became Head ancestor":        "convergence",
		"protected deletion confirmed":                    "convergence",
		"confirmed deletion resumes upload failure":       "convergence",
		"100 MiB upload resumed":                          "convergence",
		"checkout resumes truncated download":             "convergence",
		"checkout rejects wrong digest then resumes":      "convergence",
		"checkout resumes disk failure":                   "convergence",
		"initial checkout crash before":                   "convergence",
		"initial checkout crash after":                    "convergence",
		"sync transaction crash before_base_commit":       "convergence",
		"sync transaction crash after_base_commit":        "convergence",
		"sync transaction crash before_cleanup_commit":    "convergence",
		"sync transaction crash after_cleanup_commit":     "convergence",
		"Head update before":                              "server-readability",
		"Head update after":                               "server-readability",
		"bind checkout create File between identity":      "recovery",
		"bind checkout create Directory between identity": "recovery",
		"sync checkout create File between identity":      "recovery",
		"sync checkout create Directory between identity": "recovery",
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
	result["cross-platform fixed vectors"] = "fixed-vectors"
	result["deterministic convergence first client"] = "convergence"
	result["deterministic convergence second client"] = "convergence"
	result["filesystem primitives"] = "filesystem-primitives"
	for _, category := range []struct{ op, kind string }{
		{fsOpCreateFile, "File"}, {fsOpCreateDirectory, "Directory"},
		{fsOpMtime, "File"}, {fsOpMtime, "Directory"},
		{fsOpRename, "File"}, {fsOpRename, "Directory"},
	} {
		for _, point := range []string{"before_intent_commit", "after_intent_commit", "after_action", "after_parent_sync", "after_completed"} {
			result[platformBindCrashScenario(category.op, category.kind, point)] = "convergence"
		}
	}
	for _, name := range []string{"capture-file", "capture-directory", "post-base-file", "post-base-directory"} {
		for _, point := range []string{"before_intent_commit", "after_intent_commit", "after_action", "after_parent_sync", "after_completed"} {
			result[platformSyncCrashScenario(name, point)] = "convergence"
		}
	}
	return result
}

func TestCrossPlatformDeterministicConvergence(t *testing.T) {
	if _, _, enabled := acceptance.ActivePlatform(); !enabled {
		t.Skip("set FILECLOUD_RUN_1B=1 to run deterministic cross-platform convergence")
	}
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	publisherDir, publisherTree := newPlatformClientPaths(t)
	subscriberDir, subscriberTree := newPlatformClientPaths(t)
	runFixed := func(args []string, stdin string, now time.Time) error {
		if len(args) > 0 && args[0] == "library" {
			args = args[1:]
		}
		return runLibraryWithConfig(t.Context(), args, strings.NewReader(stdin), io.Discard, io.Discard,
			libraryClientConfig{now: func() time.Time { return now }})
	}
	writeFixed := func(path, content string, modified time.Time) {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, modified, modified); err != nil {
			t.Fatal(err)
		}
	}

	baseTime := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	writeFixed(filepath.Join(publisherTree, "shared.txt"), "fixed base", baseTime)
	publisherArgs := append(bindArgs(publisherDir, environment.server.URL, testClientLibraryID, publisherTree, testClientDeviceID), "--import-local")
	if err := runFixed(publisherArgs, environment.token+"\n", time.Date(2026, 8, 9, 11, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("bind deterministic publisher: %v", err)
	}
	if err := runFixed(bindArgs(subscriberDir, environment.server.URL, testClientLibraryID, subscriberTree, testOtherDeviceID),
		environment.token+"\n", time.Date(2026, 8, 9, 11, 1, 0, 0, time.UTC)); err != nil {
		t.Fatalf("bind deterministic subscriber: %v", err)
	}

	remoteTime := time.Date(2026, 8, 9, 10, 2, 0, 0, time.UTC)
	writeFixed(filepath.Join(publisherTree, "remote.txt"), "fixed remote", remoteTime)
	if err := runFixed([]string{"sync", "--client-dir", publisherDir, "--worktree", publisherTree}, "",
		time.Date(2026, 8, 9, 11, 2, 0, 0, time.UTC)); err != nil {
		t.Fatalf("publish deterministic remote change: %v", err)
	}
	localTime := time.Date(2026, 8, 9, 10, 3, 0, 0, time.UTC)
	writeFixed(filepath.Join(subscriberTree, "local.txt"), "fixed local", localTime)
	if err := runFixed([]string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree}, "",
		time.Date(2026, 8, 9, 11, 3, 0, 0, time.UTC)); err != nil {
		t.Fatalf("merge deterministic changes: %v", err)
	}
	if err := runFixed([]string{"sync", "--client-dir", publisherDir, "--worktree", publisherTree}, "",
		time.Date(2026, 8, 9, 11, 4, 0, 0, time.UTC)); err != nil {
		t.Fatalf("download deterministic merge: %v", err)
	}

	confirmed := platformConfirmedInputs("fixed base", "fixed remote", "fixed local")
	first := assertPlatformConverged(t, "deterministic convergence first client", environment, publisherDir, publisherTree, confirmed)
	second := assertPlatformConverged(t, "deterministic convergence second client", environment, subscriberDir, subscriberTree, confirmed)
	const wantRoot = "746abb68bf9a6fb9cc2251b96be467622a28dd8a7d6686c73a5f0db97a703599"
	const wantHead = "6b604c48ae82f9a829370976c3f08ce34dee7ea01a1791a336b3363175fcee53"
	if first.SyncBase != second.SyncBase || first.SyncBaseRoot != second.SyncBaseRoot ||
		first.SyncBase != wantHead || first.SyncBaseRoot != wantRoot {
		t.Fatalf("deterministic convergence first=%s/%s second=%s/%s, want %s/%s",
			first.SyncBase, first.SyncBaseRoot, second.SyncBase, second.SyncBaseRoot, wantHead, wantRoot)
	}
}

func TestPlatformCorrectnessLoop(t *testing.T) {
	if _, _, enabled := acceptance.ActivePlatform(); !enabled {
		t.Skip("set FILECLOUD_RUN_1B=1 to run the platform correctness loop")
	}
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	publisherDir, publisherTree := newPlatformClientPaths(t)
	subscriberDir, subscriberTree := newPlatformClientPaths(t)

	confirmed := []platformConfirmedInput{
		{name: "base", data: []byte("confirmed base")},
	}
	if err := os.WriteFile(filepath.Join(publisherTree, "shared.txt"), confirmed[0].data, 0o600); err != nil {
		t.Fatal(err)
	}
	bindPublisher := append(bindArgs(publisherDir, environment.server.URL, testClientLibraryID, publisherTree, testClientDeviceID), "--import-local")
	if err := runPlatformCLI(t, bindPublisher, environment.token+"\n"); err != nil {
		t.Fatalf("import publisher: %v", err)
	}
	assertPlatformConverged(t, "publisher import", environment, publisherDir, publisherTree, confirmed)

	if err := runPlatformCLI(t, bindArgs(subscriberDir, environment.server.URL, testClientLibraryID, subscriberTree, testOtherDeviceID), environment.token+"\n"); err != nil {
		t.Fatalf("bind subscriber: %v", err)
	}
	assertPlatformConverged(t, "subscriber checkout", environment, subscriberDir, subscriberTree, confirmed)

	remoteOnly := platformConfirmedInput{name: "remote independent change", data: []byte("confirmed remote independent")}
	localOnly := platformConfirmedInput{name: "local independent change", data: []byte("confirmed local independent")}
	confirmed = append(confirmed, remoteOnly, localOnly)
	if err := os.WriteFile(filepath.Join(publisherTree, "remote.txt"), remoteOnly.data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runPlatformCLI(t, []string{"library", "sync", "--client-dir", publisherDir, "--worktree", publisherTree}, ""); err != nil {
		t.Fatalf("publish remote independent change: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "local.txt"), localOnly.data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runPlatformCLI(t, []string{"library", "sync", "--client-dir", subscriberDir, "--worktree", subscriberTree}, ""); err != nil {
		t.Fatalf("merge independent changes: %v", err)
	}
	assertPlatformConverged(t, "independent merge subscriber", environment, subscriberDir, subscriberTree, confirmed)
	if err := runPlatformCLI(t, []string{"library", "sync", "--client-dir", publisherDir, "--worktree", publisherTree}, ""); err != nil {
		t.Fatalf("download independent merge: %v", err)
	}
	assertPlatformSameConvergence(t, "independent merge", environment,
		platformClient{clientDir: publisherDir, worktree: publisherTree},
		platformClient{clientDir: subscriberDir, worktree: subscriberTree}, confirmed)

	remoteConflict := platformConfirmedInput{name: "remote conflicting change", data: []byte("confirmed remote conflict")}
	localConflict := platformConfirmedInput{name: "local conflicting change", data: []byte("confirmed local conflict")}
	confirmed = append(confirmed, remoteConflict, localConflict)
	if err := os.WriteFile(filepath.Join(publisherTree, "shared.txt"), remoteConflict.data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runPlatformCLI(t, []string{"library", "sync", "--client-dir", publisherDir, "--worktree", publisherTree}, ""); err != nil {
		t.Fatalf("publish remote conflict: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "shared.txt"), localConflict.data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runPlatformCLI(t, []string{"library", "sync", "--client-dir", subscriberDir, "--worktree", subscriberTree}, ""); err != nil {
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
	if err := runPlatformCLI(t, []string{"library", "sync", "--client-dir", publisherDir, "--worktree", publisherTree}, ""); err != nil {
		t.Fatalf("download conflict result: %v", err)
	}
	assertPlatformSameConvergence(t, "conflict preservation", environment,
		platformClient{clientDir: publisherDir, worktree: publisherTree},
		platformClient{clientDir: subscriberDir, worktree: subscriberTree}, confirmed)

	assertPlatformOwnerIsolation(t, environment)
}

type platformConfirmedInput struct {
	name   string
	data   []byte
	digest string
}

func (i platformConfirmedInput) contentDigest() string {
	if i.digest != "" {
		return i.digest
	}
	return object.ID(i.data)
}

func platformConfirmedInputs(values ...string) []platformConfirmedInput {
	result := make([]platformConfirmedInput, 0, len(values))
	for index, value := range values {
		result = append(result, platformConfirmedInput{name: fmt.Sprintf("input-%d", index+1), data: []byte(value)})
	}
	return result
}

func platformConfirmedFiles(files map[string][]byte) []platformConfirmedInput {
	names := slices.Sorted(maps.Keys(files))
	result := make([]platformConfirmedInput, 0, len(names))
	for _, name := range names {
		result = append(result, platformConfirmedInput{name: name, data: files[name]})
	}
	return result
}

type platformClient struct {
	clientDir string
	worktree  string
}

type platformReachability struct {
	objects int
	files   map[string]string
}

func runPlatformCLI(t *testing.T, args []string, stdin string) error {
	t.Helper()
	return run(t.Context(), args, bytes.NewBufferString(stdin), io.Discard, io.Discard)
}

func newPlatformClientPaths(t *testing.T) (string, string) {
	t.Helper()
	clientDir := filepath.Join(t.TempDir(), "client")
	root := acceptance.Root()
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
	platform, filesystem, enabled := acceptance.ActivePlatform()
	if enabled {
		requireAcceptanceFilesystem(t, canonical, platform, filesystem)
	}
	return clientDir, canonical
}

func assertPlatformSameConvergence(t *testing.T, scenario string, environment libraryCLIEnvironment,
	first, second platformClient, confirmed []platformConfirmedInput,
) {
	t.Helper()
	firstBinding := assertPlatformConverged(t, scenario+" first client", environment, first.clientDir, first.worktree, confirmed)
	secondBinding := assertPlatformConverged(t, scenario+" second client", environment, second.clientDir, second.worktree, confirmed)
	if firstBinding.SyncBase != secondBinding.SyncBase || firstBinding.SyncBaseRoot != secondBinding.SyncBaseRoot {
		t.Fatalf("%s clients differ: first=%+v second=%+v", scenario, firstBinding, secondBinding)
	}
}

func assertPlatformConverged(t *testing.T, scenario string, environment libraryCLIEnvironment,
	clientDir, worktree string, confirmed []platformConfirmedInput,
) clientBinding {
	t.Helper()
	return assertPlatformConvergence(t, scenario, environment, clientDir, worktree, confirmed, true)
}

func assertPlatformConvergedWithoutAttestation(t *testing.T, scenario string, environment libraryCLIEnvironment,
	clientDir, worktree string, confirmed []platformConfirmedInput,
) clientBinding {
	t.Helper()
	return assertPlatformConvergence(t, scenario, environment, clientDir, worktree, confirmed, false)
}

func assertPlatformConvergence(t *testing.T, scenario string, environment libraryCLIEnvironment,
	clientDir, worktree string, confirmed []platformConfirmedInput, emit bool,
) clientBinding {
	t.Helper()
	platform, filesystem, enabled := acceptance.ActivePlatform()
	if enabled {
		requireAcceptanceFilesystem(t, worktree, platform, filesystem)
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
	var rootStat fscompat.Stat_t
	if err := fscompat.Lstat(worktree, &rootStat); err != nil {
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
	owner, err := getLibraryOwner(t.Context(), base, testClientLibraryID, []byte(environment.token))
	if err != nil {
		t.Fatalf("%s read library owner: %v", scenario, err)
	}
	reachable := inspectPlatformReachability(t, environment, *head.CommitID, owner)
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
	if emit {
		emitPlatformAttestation(t, platformAttestation{
			Kind: "convergence", Scenario: scenario, Platform: platform, Filesystem: filesystem,
			Head: *head.CommitID, SyncBase: binding.SyncBase, HeadRoot: commit.Root, BaseRoot: binding.SyncBaseRoot,
			Snapshot: snapshot.root, ReachableObjects: reachable.objects, ConfirmedInputDigests: confirmedDigests,
			PreservedInputDigests: preservedDigests, ResidualJournalRows: residualRows,
		})
	}
	return binding
}

func emitPlatformAttestation(t *testing.T, attestation platformAttestation) {
	t.Helper()
	if _, _, enabled := acceptance.ActivePlatform(); !enabled {
		return
	}
	line, err := acceptance.Encode(attestation)
	if err != nil {
		t.Fatalf("encode platform attestation: %v", err)
	}
	t.Log(line)
}

func inspectPlatformReachability(t *testing.T, environment libraryCLIEnvironment, head, expectedOwner string) platformReachability {
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
		if err != nil || commit.AuthorUserID != expectedOwner {
			t.Fatalf("verify reachable commit %s: commit=%+v expected_owner=%s err=%v", commitID, commit, expectedOwner, err)
		}
		visitDirectory(commit.Root)
		pending = append(pending, commit.Parents...)
	}
	return platformReachability{objects: len(visited), files: files}
}

func assertPlatformOwnerIsolation(t *testing.T, environment libraryCLIEnvironment) {
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
		foreignStatus, foreignBody := platformRawRequest(t, base.JoinPath("v1/libraries", testClientLibraryID).String()+path,
			http.MethodGet, otherToken, nil)
		missingStatus, missingBody := platformRawRequest(t, base.JoinPath("v1/libraries", missingID).String()+path,
			http.MethodGet, otherToken, nil)
		if foreignStatus != http.StatusNotFound || missingStatus != http.StatusNotFound || !bytes.Equal(foreignBody, missingBody) {
			t.Fatalf("owner isolation path %q differs: foreign=%d/%q missing=%d/%q", path, foreignStatus, foreignBody, missingStatus, missingBody)
		}
	}

	createBody := []byte(`{"Name":"Bob isolated"}`)
	status, _ := platformRawRequest(t, base.JoinPath("v1/libraries", testClientLibraryID).String(), http.MethodPut, otherToken, createBody)
	if status != http.StatusCreated {
		t.Fatalf("same LibraryId in isolated owner namespace returned %d", status)
	}
	otherHead, err := getRemoteHead(t.Context(), base, testClientLibraryID, []byte(otherToken))
	if err != nil || otherHead.CommitID != nil || otherHead.ETag != `"head-version-0"` {
		t.Fatalf("isolated owner Head=%+v err=%v", otherHead, err)
	}
	for _, id := range []string{*ownerHead.CommitID, missingObjID} {
		status, _ := platformRawRequest(t, base.JoinPath("v1/libraries", testClientLibraryID, "objects", "commits", id).String(),
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
	status, _ = platformRawRequest(t, base.JoinPath("v1/libraries", testClientLibraryID, "blocks", otherBlockID).String(),
		http.MethodGet, otherToken, nil)
	if status != http.StatusOK {
		t.Fatalf("other owner block GET returned %d", status)
	}
	status, _ = platformRawRequest(t, base.JoinPath("v1/libraries", testClientLibraryID, "blocks", otherBlockID).String(),
		http.MethodGet, environment.token, nil)
	if status != http.StatusNotFound {
		t.Fatalf("original owner saw other owner's block: status=%d", status)
	}
	currentOwner, err := getRemoteHead(t.Context(), base, testClientLibraryID, []byte(environment.token))
	if err != nil || currentOwner.CommitID == nil || *currentOwner.CommitID != *ownerHead.CommitID {
		t.Fatalf("other owner changed original Head: before=%+v after=%+v err=%v", ownerHead, currentOwner, err)
	}
	platform, filesystem, _ := acceptance.ActivePlatform()
	emitPlatformAttestation(t, platformAttestation{
		Kind: "isolation", Scenario: "two-user isolation", Platform: platform, Filesystem: filesystem,
		OwnerHead: *ownerHead.CommitID, Isolation: "uniform-not-found",
	})
}

func platformRawRequest(t *testing.T, target, method, token string, body []byte) (int, []byte) {
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
