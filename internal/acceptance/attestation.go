// Package acceptance defines structured evidence emitted by platform gates.
package acceptance

import (
	"encoding/json"
	"os"
)

// Prefix identifies a structured acceptance attestation in test output.
const Prefix = "FILECLOUD_ATTESTATION "

// Attestation records one platform convergence, isolation, readability, or filesystem primitive proof.
type Attestation struct {
	Kind                      string   `json:"kind"`
	Scenario                  string   `json:"scenario"`
	Platform                  string   `json:"platform"`
	Filesystem                string   `json:"filesystem"`
	Head                      string   `json:"head,omitempty"`
	SyncBase                  string   `json:"syncBase,omitempty"`
	HeadRoot                  string   `json:"headRoot,omitempty"`
	BaseRoot                  string   `json:"baseRoot,omitempty"`
	Snapshot                  string   `json:"snapshot,omitempty"`
	ReachableObjects          int      `json:"reachableObjects,omitempty"`
	ConfirmedInputDigests     []string `json:"confirmedInputDigests,omitempty"`
	PreservedInputDigests     []string `json:"preservedInputDigests,omitempty"`
	UnregisteredInternalPaths int      `json:"unregisteredInternalPaths"`
	ResidualJournalRows       int      `json:"residualJournalRows"`
	OwnerHead                 string   `json:"ownerHead,omitempty"`
	OtherHead                 *string  `json:"otherHead,omitempty"`
	Isolation                 string   `json:"isolation,omitempty"`
	FailurePoint              string   `json:"failurePoint,omitempty"`
	OldHead                   string   `json:"oldHead,omitempty"`
	CurrentHead               string   `json:"currentHead,omitempty"`
	PreviousSyncBase          string   `json:"previousSyncBase,omitempty"`
	NoFollow                  bool     `json:"noFollow,omitempty"`
	StableFileIdentity        bool     `json:"stableFileIdentity,omitempty"`
	NoReplaceRename           bool     `json:"noReplaceRename,omitempty"`
	NoReplaceLink             bool     `json:"noReplaceLink,omitempty"`
	SameDirectoryRename       bool     `json:"sameDirectoryRename,omitempty"`
	DirectorySync             bool     `json:"directorySync,omitempty"`
	CrossProcessLock          bool     `json:"crossProcessLock,omitempty"`
	OldFDWritesDetached       bool     `json:"oldFdWritesDetached,omitempty"`
	Warning                   string   `json:"warning,omitempty"`
}

// ActivePlatform returns the explicitly selected acceptance platform.
func ActivePlatform() (platform, filesystem string, enabled bool) {
	if os.Getenv("FILECLOUD_RUN_1A") == "1" {
		return "linux", "ext4", true
	}
	if os.Getenv("FILECLOUD_RUN_1B_APFS") == "1" {
		return "darwin", "apfs", true
	}
	return "", "", false
}

// Root returns the verified filesystem root supplied by the active gate.
func Root() string {
	if root := os.Getenv("FILECLOUD_ACCEPTANCE_ROOT"); root != "" {
		return root
	}
	return os.Getenv("FILECLOUD_LINUX_EXT4_ROOT")
}

// Encode returns the prefixed JSON line consumed by platform gates.
func Encode(attestation Attestation) (string, error) {
	data, err := json.Marshal(attestation)
	if err != nil {
		return "", err
	}
	return Prefix + string(data), nil
}
