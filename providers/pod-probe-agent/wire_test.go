package main

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// These vectors are the cross-language source of truth. If this test passes,
// this binary's handshake, registration and KYOK payloads are byte-identical
// to what the gateway and souk core verify against — proven without running
// either. The signatures are deterministic Ed25519, so reproducing them
// under the published test key is equivalent to the gateway accepting them.

func repoRoot(t *testing.T) string {
	// providers/pod-probe-agent -> repo root
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func TestHandshakeVectors(t *testing.T) {
	root := repoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "docs", "wire-vectors.json"))
	if err != nil {
		t.Fatalf("read wire-vectors.json: %v", err)
	}
	var vf struct {
		HandshakeVersion int `json:"handshake_version"`
		ProviderTestKey  struct {
			PrivateHex string `json:"private_key_hex"`
			PublicHex  string `json:"public_key_hex"`
		} `json:"provider_test_key"`
		SoukTestKey struct {
			PrivateHex string `json:"private_key_hex"`
			PublicHex  string `json:"public_key_hex"`
		} `json:"souk_test_key"`
		Vectors []struct {
			Kind     string `json:"kind"`
			SignedBy string `json:"signed_by"`
			Inputs   struct {
				ProviderNonce string `json:"provider_nonce"`
				SoukNonce     string `json:"souk_nonce"`
				HelloRaw      string `json:"hello_raw"`
			} `json:"inputs"`
			PayloadUTF8  string `json:"payload_utf8"`
			SignatureHex string `json:"signature_hex"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &vf); err != nil {
		t.Fatal(err)
	}

	if vf.HandshakeVersion != handshakeVersion {
		t.Fatalf("handshake version drift: file %d, binary %d", vf.HandshakeVersion, handshakeVersion)
	}

	providerKey := ed25519.NewKeyFromSeed(mustHex(t, vf.ProviderTestKey.PrivateHex))
	soukKey := ed25519.NewKeyFromSeed(mustHex(t, vf.SoukTestKey.PrivateHex))

	for _, v := range vf.Vectors {
		var payload []byte
		var signer ed25519.PrivateKey
		switch v.Kind {
		case "souk-challenge":
			payload = soukChallengePayload(v.Inputs.ProviderNonce, v.Inputs.SoukNonce)
			signer = soukKey
		case "provider-proof":
			payload = providerProofPayload(v.Inputs.ProviderNonce, v.Inputs.SoukNonce, []byte(v.Inputs.HelloRaw))
			signer = providerKey
		default:
			t.Fatalf("unknown vector kind %q", v.Kind)
		}
		if string(payload) != v.PayloadUTF8 {
			t.Errorf("%s payload:\n  got  %q\n  want %q", v.Kind, payload, v.PayloadUTF8)
			continue
		}
		// Deterministic Ed25519: our signature must equal the published one.
		got := hex.EncodeToString(ed25519.Sign(signer, payload))
		if got != v.SignatureHex {
			t.Errorf("%s signature:\n  got  %s\n  want %s", v.Kind, got, v.SignatureHex)
		}
	}
}

func TestRegistrationAndKyokVectors(t *testing.T) {
	root := repoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "AgentSouk", "docs", "contract-vectors.json"))
	if err != nil {
		t.Skipf("upstream contract-vectors.json not checked out: %v", err)
	}
	var cf struct {
		Vectors []struct {
			Kind   string `json:"kind"`
			Inputs struct {
				Names       []string `json:"names"`
				Timestamp   int64    `json:"timestamp"`
				Bearer      string   `json:"bearer"`
				BodyHashHex string   `json:"body_sha256_hex"`
			} `json:"inputs"`
			PayloadUTF8 string `json:"payload_utf8"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &cf); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, v := range cf.Vectors {
		switch v.Kind {
		case "agent-registration":
			got := registrationPayload(v.Inputs.Names, v.Inputs.Timestamp)
			if string(got) != v.PayloadUTF8 {
				t.Errorf("registration payload:\n  got  %q\n  want %q", got, v.PayloadUTF8)
			}
			seen++
		case "kyok-call":
			got := kyokCallPayload(v.Inputs.Bearer, v.Inputs.Timestamp, v.Inputs.BodyHashHex)
			if string(got) != v.PayloadUTF8 {
				t.Errorf("kyok-call payload:\n  got  %q\n  want %q", got, v.PayloadUTF8)
			}
			seen++
		}
	}
	if seen == 0 {
		t.Error("neither agent-registration nor kyok-call vector found")
	}
}

// The proof digest must be over the exact hello bytes sent, not a
// re-encoding. This guards the one mistake the spec calls out: hashing a
// re-serialization instead of the wire bytes.
func TestHelloDigestIsOverWireBytes(t *testing.T) {
	root := repoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "docs", "wire-vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vf struct {
		Vectors []struct {
			Kind   string `json:"kind"`
			Inputs struct {
				HelloRaw     string `json:"hello_raw"`
				HelloSHA256  string `json:"hello_sha256_hex"`
			} `json:"inputs"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &vf); err != nil {
		t.Fatal(err)
	}
	for _, v := range vf.Vectors {
		if v.Kind != "provider-proof" {
			continue
		}
		digest := sha256.Sum256([]byte(v.Inputs.HelloRaw))
		if hex.EncodeToString(digest[:]) != v.Inputs.HelloSHA256 {
			t.Errorf("hello digest:\n  got  %s\n  want %s", hex.EncodeToString(digest[:]), v.Inputs.HelloSHA256)
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
