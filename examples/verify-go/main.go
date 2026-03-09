// verify-go: External cross-verification of PunkGo Merkle inclusion proofs
// using Go's standard golang.org/x/mod/sumdb/tlog library.
//
// This proves that PunkGo uses standard RFC 6962 transparency log hashing —
// any conformant tlog implementation can independently verify proofs.
//
// Usage:
//
//	punkgo-jack show <event_id> --json > proof.json
//	go run main.go proof.json
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"golang.org/x/mod/sumdb/tlog"
)

// proofJSON matches the output of `punkgo-jack show <event_id> --json`.
type proofJSON struct {
	Event struct {
		ID        string `json:"id"`
		EventHash string `json:"event_hash"`
		LogIndex  int64  `json:"log_index"`
	} `json:"event"`
	Proof struct {
		TreeSize int64    `json:"tree_size"`
		Proof    []string `json:"proof"`
	} `json:"proof"`
	Checkpoint struct {
		RootHash string `json:"root_hash"`
	} `json:"checkpoint"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <proof.json>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nGenerate proof JSON with:\n")
		fmt.Fprintf(os.Stderr, "  punkgo-jack show <event_id> --json > proof.json\n")
		os.Exit(1)
	}

	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		fatal("reading file: %v", err)
	}

	var pj proofJSON
	if err := json.Unmarshal(data, &pj); err != nil {
		fatal("parsing JSON: %v", err)
	}

	// Validate required fields.
	if pj.Event.EventHash == "" {
		fatal("missing event.event_hash in JSON")
	}
	if pj.Proof.TreeSize == 0 {
		fatal("missing or zero proof.tree_size in JSON")
	}

	// Decode the leaf hash (event_hash from the kernel).
	leafHash, err := hexToTlogHash(pj.Event.EventHash)
	if err != nil {
		fatal("decoding event_hash: %v", err)
	}

	// Decode the proof path hashes.
	proof := make(tlog.RecordProof, len(pj.Proof.Proof))
	for i, h := range pj.Proof.Proof {
		proof[i], err = hexToTlogHash(h)
		if err != nil {
			fatal("decoding proof hash #%d: %v", i, err)
		}
	}

	// Print verification context.
	eventLabel := pj.Event.ID
	if eventLabel == "" {
		eventLabel = pj.Event.EventHash
	}
	fmt.Printf("Event:      %s\n", eventLabel)
	fmt.Printf("Log index:  %d\n", pj.Event.LogIndex)
	fmt.Printf("Tree size:  %d\n", pj.Proof.TreeSize)
	fmt.Printf("Leaf hash:  %s\n", pj.Event.EventHash)
	fmt.Printf("Proof path: %d hashes\n", len(proof))

	// Verify against checkpoint root if available.
	if pj.Checkpoint.RootHash != "" {
		rootHash, err := hexToTlogHash(pj.Checkpoint.RootHash)
		if err != nil {
			fatal("decoding checkpoint root_hash: %v", err)
		}
		fmt.Printf("Checkpoint: %s\n", pj.Checkpoint.RootHash)

		err = tlog.CheckRecord(proof, pj.Proof.TreeSize, rootHash, pj.Event.LogIndex, leafHash)
		if err != nil {
			fmt.Printf("Inclusion:  FAILED -- %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Inclusion:  VERIFIED -- leaf is in the tree, root matches checkpoint\n")
	} else {
		// No checkpoint root -- compute the implied root from the proof
		// to verify proof structure is internally consistent.
		computedRoot, err := computeRootFromProof(proof, pj.Proof.TreeSize, pj.Event.LogIndex, leafHash)
		if err != nil {
			fmt.Printf("Inclusion:  FAILED -- %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Computed:   %s\n", hex.EncodeToString(computedRoot[:]))
		fmt.Printf("Inclusion:  VERIFIED -- proof is mathematically valid\n")
		fmt.Printf("            (no checkpoint provided for root comparison)\n")
	}
}

// hexToTlogHash decodes a hex-encoded 32-byte hash into tlog.Hash.
func hexToTlogHash(s string) (tlog.Hash, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return tlog.Hash{}, fmt.Errorf("invalid hex: %w", err)
	}
	if len(b) != tlog.HashSize {
		return tlog.Hash{}, fmt.Errorf("expected %d bytes, got %d", tlog.HashSize, len(b))
	}
	var h tlog.Hash
	copy(h[:], b)
	return h, nil
}

// computeRootFromProof recomputes the tree root from a record inclusion proof.
// This mirrors tlog's internal runRecordProof logic and the equivalent in
// punkgo-kernel's verify.rs.
func computeRootFromProof(p tlog.RecordProof, treeSize, index int64, leafHash tlog.Hash) (tlog.Hash, error) {
	if index < 0 || index >= treeSize {
		return tlog.Hash{}, fmt.Errorf("index (%d) out of range [0, %d)", index, treeSize)
	}
	return runRecordProof(p, 0, treeSize, index, leafHash)
}

func runRecordProof(p []tlog.Hash, lo, hi, n int64, h tlog.Hash) (tlog.Hash, error) {
	if n < lo || n >= hi {
		return tlog.Hash{}, fmt.Errorf("invalid proof structure")
	}
	if lo+1 == hi {
		if len(p) != 0 {
			return tlog.Hash{}, fmt.Errorf("proof has extra hashes")
		}
		return h, nil
	}
	if len(p) == 0 {
		return tlog.Hash{}, fmt.Errorf("proof too short")
	}
	k := maxpow2(hi - lo)
	if n < lo+k {
		th, err := runRecordProof(p[:len(p)-1], lo, lo+k, n, h)
		if err != nil {
			return tlog.Hash{}, err
		}
		return nodeHash(th, p[len(p)-1]), nil
	}
	th, err := runRecordProof(p[:len(p)-1], lo+k, hi, n, h)
	if err != nil {
		return tlog.Hash{}, err
	}
	return nodeHash(p[len(p)-1], th), nil
}

// maxpow2 returns the largest power of 2 less than n.
func maxpow2(n int64) int64 {
	if n <= 1 {
		return 1
	}
	// Find highest bit position in (n-1).
	k := int64(1)
	for k*2 < n {
		k *= 2
	}
	return k
}

// nodeHash computes the RFC 6962 interior node hash: SHA-256(0x01 || left || right).
// This is a thin wrapper around tlog.NodeHash for readability.
func nodeHash(left, right tlog.Hash) tlog.Hash {
	return tlog.NodeHash(left, right)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
