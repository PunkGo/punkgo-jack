# verify-go

External cross-verification of PunkGo Merkle inclusion proofs using Go's
standard `golang.org/x/mod/sumdb/tlog` library.

## What this is

PunkGo's kernel produces RFC 6962 transparency log proofs for every recorded
event. This Go program verifies those proofs using a completely independent
implementation — Go's `sumdb/tlog` package (the same library that backs
`go mod` supply-chain verification).

If this program says "VERIFIED", it means:
- The event hash is included in the Merkle tree at the claimed index
- The proof path is mathematically valid against the checkpoint root
- PunkGo's Rust implementation and Go's `tlog` library agree on the math

## Usage

```bash
# Export a proof from PunkGo
punkgo-jack show <event_id> --json > proof.json

# Verify with Go's tlog
cd examples/verify-go
go run main.go proof.json
```

Expected output:
```
Event:      abc123...
Log index:  42
Tree size:  100
Leaf hash:  <hex>
Proof path: 7 hashes
Checkpoint: <hex>
Inclusion:  VERIFIED -- leaf is in the tree, root matches checkpoint
```

## Why this matters

PunkGo does not use a proprietary proof format. It uses RFC 6962, the same
standard behind Certificate Transparency, Go's module checksum database, and
Sigstore. Any tlog-compatible verifier — in any language — can independently
check PunkGo proofs without trusting PunkGo's own code.

This is the difference between **trust-based** and **verification-based**
accountability.

## JSON format

The input JSON matches `punkgo-jack show <event_id> --json` output:

```json
{
  "event": {
    "id": "abc123...",
    "event_hash": "<hex-encoded-sha256>",
    "log_index": 42
  },
  "proof": {
    "tree_size": 100,
    "proof": ["<hex-hash>", "<hex-hash>", "..."]
  },
  "checkpoint": {
    "root_hash": "<hex-encoded-root>"
  }
}
```
