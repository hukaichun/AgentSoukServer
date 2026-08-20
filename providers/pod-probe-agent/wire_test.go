package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// These vectors are the cross-language source of truth. If these tests pass,
// this binary's handshake, registration and KYOK payloads are byte-identical
// to what souk core verifies against — proven without running it. The
// signatures are deterministic Ed25519, so reproducing them under the
// published test key is equivalent to souk accepting them.
//
// The payload bytes live upstream (AgentSouk/docs/contract-vectors.json);
// this repo's docs/wire-vectors.json keeps only the handshake version and
// the frame vocabularies, per its own comment.

func repoRoot(t *testing.T) string {
	// providers/pod-probe-agent -> repo root
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func TestHandshakeVersionMatchesPublished(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "docs", "wire-vectors.json"))
	if err != nil {
		t.Fatalf("read wire-vectors.json: %v", err)
	}
	var vf struct {
		HandshakeVersion int `json:"handshake_version"`
	}
	if err := json.Unmarshal(data, &vf); err != nil {
		t.Fatal(err)
	}
	if vf.HandshakeVersion != handshakeVersion {
		t.Fatalf("handshake version drift: file %d, binary %d", vf.HandshakeVersion, handshakeVersion)
	}
}

func TestContractVectors(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "AgentSouk", "docs", "contract-vectors.json"))
	if err != nil {
		t.Skipf("upstream contract-vectors.json not checked out: %v", err)
	}
	var cf struct {
		TestKey struct {
			PrivateHex string `json:"private_key_hex"`
		} `json:"test_key"`
		Vectors []struct {
			Kind   string `json:"kind"`
			Inputs struct {
				SoukPublicKey string   `json:"souk_public_key"`
				SoukNonce     string   `json:"souk_nonce"`
				ProviderNonce string   `json:"provider_nonce"`
				Names         []string `json:"names"`
				Timestamp     int64    `json:"timestamp"`
				Bearer        string   `json:"bearer"`
				BodyHashHex   string   `json:"body_sha256_hex"`
			} `json:"inputs"`
			PayloadUTF8  string `json:"payload_utf8"`
			SignatureHex string `json:"signature_hex"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &cf); err != nil {
		t.Fatal(err)
	}
	testKey := ed25519.NewKeyFromSeed(mustHex(t, cf.TestKey.PrivateHex))
	seen := map[string]bool{}
	for _, v := range cf.Vectors {
		var payload []byte
		switch v.Kind {
		case "agent-registration":
			payload = registrationPayload(v.Inputs.Names, v.Inputs.Timestamp)
		case "kyok-call":
			payload = kyokCallPayload(v.Inputs.Bearer, v.Inputs.Timestamp, v.Inputs.BodyHashHex)
		case "provider-connect":
			payload = providerConnectPayload(v.Inputs.SoukPublicKey, v.Inputs.SoukNonce, v.Inputs.ProviderNonce, v.Inputs.Names)
		case "souk-connect":
			payload = soukConnectPayload(v.Inputs.SoukNonce, v.Inputs.ProviderNonce)
		default:
			continue // families this binary never signs or checks
		}
		seen[v.Kind] = true
		if string(payload) != v.PayloadUTF8 {
			t.Errorf("%s payload:\n  got  %q\n  want %q", v.Kind, payload, v.PayloadUTF8)
			continue
		}
		if v.SignatureHex != "" {
			// Deterministic Ed25519: our signature over the same bytes
			// under the published key must equal the published one.
			got := hex.EncodeToString(ed25519.Sign(testKey, payload))
			if got != v.SignatureHex {
				t.Errorf("%s signature:\n  got  %s\n  want %s", v.Kind, got, v.SignatureHex)
			}
		}
	}
	for _, kind := range []string{"agent-registration", "kyok-call", "provider-connect", "souk-connect"} {
		if !seen[kind] {
			t.Errorf("no %s vector found — the family this binary signs is unvectored", kind)
		}
	}
}

func mustHex(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
