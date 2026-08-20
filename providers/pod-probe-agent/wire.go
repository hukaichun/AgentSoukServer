// The whole of what a pod-probe agent is on the wire, written against
// docs/server-mode.md and docs/wire-vectors.json rather than against any
// SDK: this binary is dropped into a pod with a souk URL and a pinned key,
// and must not need the gateway, souk core, or any Python installed to
// come alive. Every payload here is cross-checked byte-for-byte in
// wire_test.go against the published vectors.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// The handshake this binary speaks. A mismatch is refused by name on the
// souk side rather than failing as a bad signature (docs/server-mode.md).
// v3: the proof binds the recipient souk's public key, and souk requires
// a proof unconditionally.
const handshakeVersion = 3

// registrationPayload is the exact bytes core verifies an /agents/register
// signature against: souk.identity.registration_signing_payload, the
// `souk-register` tag over the sorted names and the timestamp. The tag is
// what keeps a registration signature from being replayable as a deletion
// order. Names are sorted so the order a caller lists them in cannot change
// the bytes. (The agent SDK in this repo omits the tag; core, the verifier,
// requires it — this matches the verifier.)
func registrationPayload(names []string, timestamp int64) []byte {
	return []byte(fmt.Sprintf("souk-register:%s:%d", strings.Join(sortedCopy(names), ","), timestamp))
}

// soukConnectPayload is what souk signs so this side can tell one souk
// from another answering the same URL — the half of the exchange souk
// never had before (docs/server-mode.md, "souk signs first"). Upstream's
// souk-connect family (souk.identity.souk_connect_signing_payload),
// vectored in AgentSouk/docs/contract-vectors.json.
func soukConnectPayload(soukNonce, providerNonce string) []byte {
	return []byte("souk-connect-souk:" + soukNonce + ":" + providerNonce)
}

// providerConnectPayload is what this side signs to prove it holds its key
// *now*, and to whom: soukPublicKey names the recipient — the key the souk
// just proved (empty for a souk with no identity) — so a proof produced
// for one souk cannot be relayed to attach at another. The sorted names
// bind which agents this socket attaches for; both nonces make a recorded
// exchange worthless. (souk.identity.provider_connect_signing_payload.)
func providerConnectPayload(soukPublicKey, soukNonce, providerNonce string, names []string) []byte {
	return []byte("souk-connect-provider:" + soukPublicKey + ":" + soukNonce + ":" + providerNonce + ":" + strings.Join(sortedCopy(names), ","))
}

// kyokCallPayload is what one Keep Your Own Key completion call signs: the
// run-scoped bearer token, when, and a hash of the exact request body, so a
// captured signature is neither replayable against a different body nor
// usable past the freshness window (souk_provider_sdk.identity.kyok_call_payload).
func kyokCallPayload(bearer string, timestamp int64, bodyHash string) []byte {
	return []byte(fmt.Sprintf("souk-kyok-call:%s:%d:%s", bearer, timestamp, bodyHash))
}

// helloFrame: frame one of the handshake. Nothing signs its bytes any
// more (v2 dropped the hello digest for the sorted-names binding), so the
// encoding only has to parse, not stay byte-stable. maxConcurrentRuns is
// a pointer so it can be omitted, though this agent always sends one.
type helloFrame struct {
	Type              string   `json:"type"`
	Version           int      `json:"version"`
	PublicKey         string   `json:"publicKey"`
	AgentNames        []string `json:"agentNames"`
	MaxConcurrentRuns *int     `json:"maxConcurrentRuns,omitempty"`
	Nonce             string   `json:"nonce"`
}

// SoukConn is one provider joined to one souk over one socket: runs arrive
// on it, events and acks leave by it. There is no separate "link" object
// because over a wire the two directions are literally the same socket.
type SoukConn struct {
	id         *Identity
	soukPubKey ed25519.PublicKey // pinned; nil means "accept whatever answers", logged
	agentNames []string
	maxRuns    int
	ws         *websocket.Conn
}

// handshake runs the four frames — hello, challenge, proof, welcome — and
// returns once welcome is read, or an error naming which frame failed.
//
// It does *not* read past welcome. The one ordering trap here is the
// opposite one: the caller must not then "read exactly one frame" expecting
// a welcome, because the broker begins offering inside attach, so the frame
// after proof may already be a run. This function consumes exactly welcome
// and hands the socket back with the read loop yet to start — see run().
func (c *SoukConn) handshake(ctx context.Context) error {
	providerNonce := newNonce()
	maxRuns := c.maxRuns
	hello := helloFrame{
		Type:              "hello",
		Version:           handshakeVersion,
		PublicKey:         c.id.PublicHex(),
		AgentNames:        c.agentNames,
		MaxConcurrentRuns: &maxRuns,
		Nonce:             providerNonce,
	}
	helloRaw, err := json.Marshal(hello)
	if err != nil {
		return fmt.Errorf("marshal hello: %w", err)
	}
	if err := c.ws.Write(ctx, websocket.MessageText, helloRaw); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}

	var challenge struct {
		Type          string `json:"type"`
		SoukPublicKey string `json:"soukPublicKey"`
		Nonce         string `json:"nonce"`
		Signature     string `json:"signature"`
	}
	if err := c.readJSON(ctx, &challenge); err != nil {
		return fmt.Errorf("read challenge: %w", err)
	}
	if challenge.Type != "challenge" {
		return fmt.Errorf("expected challenge, got %q", challenge.Type)
	}
	if err := c.verifySouk(challenge.SoukPublicKey, challenge.Signature, providerNonce, challenge.Nonce); err != nil {
		return err
	}

	// The proof names its recipient: the key the souk just proved, empty
	// for a souk with no identity — matching what core builds to verify.
	proofSig := c.id.Sign(providerConnectPayload(challenge.SoukPublicKey, challenge.Nonce, providerNonce, c.agentNames))
	proof, _ := json.Marshal(map[string]string{"type": "proof", "signature": proofSig})
	if err := c.ws.Write(ctx, websocket.MessageText, proof); err != nil {
		return fmt.Errorf("send proof: %w", err)
	}

	var welcome struct {
		Type string `json:"type"`
	}
	if err := c.readJSON(ctx, &welcome); err != nil {
		return fmt.Errorf("read welcome: %w", err)
	}
	if welcome.Type != "welcome" {
		return fmt.Errorf("expected welcome, got %q", welcome.Type)
	}
	return nil
}

// verifySouk decides whether to trust whatever answered the URL. A pinned
// key that does not match, or a signature that does not verify, is a hard
// stop — the whole point of pinning. An unpinned agent still checks that
// the presented key signed the challenge (enough to notice a broken souk),
// and tolerates a souk with no identity at all (soukPublicKey empty), which
// is what today's deployments without SOUK_IDENTITY_PRIVATE_KEY report.
func (c *SoukConn) verifySouk(presentedHex, signatureHex, providerNonce, soukNonce string) error {
	if presentedHex == "" {
		if c.soukPubKey != nil {
			return errors.New("souk presented no identity but a key was pinned")
		}
		logf("warning: souk presented no identity; connecting without proof of who answered")
		return nil
	}
	presented, err := hex.DecodeString(presentedHex)
	if err != nil || len(presented) != ed25519.PublicKeySize {
		return fmt.Errorf("souk public key is not a valid ed25519 key")
	}
	if c.soukPubKey != nil && !c.soukPubKey.Equal(ed25519.PublicKey(presented)) {
		return fmt.Errorf("souk is %s…, not the pinned %s…", presentedHex[:16], hex.EncodeToString(c.soukPubKey)[:16])
	}
	sig, err := hex.DecodeString(signatureHex)
	if err != nil {
		return errors.New("souk challenge signature is not hex")
	}
	if !ed25519.Verify(ed25519.PublicKey(presented), soukConnectPayload(soukNonce, providerNonce), sig) {
		return errors.New("souk challenge signature does not verify")
	}
	if c.soukPubKey == nil {
		logf("connected to souk %s… (pin this via SOUK_PUBLIC_KEY to refuse a substitute)", presentedHex[:16])
	}
	return nil
}

func (c *SoukConn) readJSON(ctx context.Context, v any) error {
	_, data, err := c.ws.Read(ctx)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// register proves this identity holds its key and states the names it
// serves. Nothing comes back that must be kept: souk mints no ids.
func register(ctx context.Context, httpURL string, id *Identity, names []string, providerName string) error {
	timestamp := time.Now().Unix()
	body := map[string]any{
		"agents":     agentRecords(names),
		"public_key": id.PublicHex(),
		"signature":  id.Sign(registrationPayload(names, timestamp)),
		"timestamp":  timestamp,
	}
	if providerName != "" {
		body["provider_name"] = providerName
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(httpURL, "/")+"/agents/register", strings.NewReader(string(raw)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("register: souk answered %s", resp.Status)
	}
	return nil
}

func agentRecords(names []string) []map[string]any {
	records := make([]map[string]any, 0, len(names))
	for _, name := range names {
		records = append(records, map[string]any{
			"name":        name,
			"description": "Read-only state probe living inside a pod: reports file build/modify times, directory listings, bounded file reads, process and env facts. Cannot write, exec, or change anything.",
		})
	}
	return records
}
