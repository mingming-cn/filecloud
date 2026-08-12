// Package acceptance defines structured evidence emitted by platform gates.
package acceptance

import "encoding/json"

// Prefix identifies a structured acceptance attestation in test output.
const Prefix = "FILECLOUD_ATTESTATION "

// Attestation records one Linux/ext4 convergence, isolation, or readability proof.
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
}

// Encode returns the prefixed JSON line consumed by the Linux/ext4 gate.
func Encode(attestation Attestation) (string, error) {
	data, err := json.Marshal(attestation)
	if err != nil {
		return "", err
	}
	return Prefix + string(data), nil
}
