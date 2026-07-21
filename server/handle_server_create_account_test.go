package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/events"
	"github.com/haileyok/cocoon/identity"
)

// TestCreateAccountInitializesRepoForExistingDID reproduces the bug where an
// account created with an existing DID (the self-custodied-rotation-key flow:
// mint your own did:plc, then pass it to createAccount alongside a
// service-auth JWT) never got a genesis repo commit. handleCreateAccount only
// ran that initialization inside `if request.Did == nil`, so any account
// created via the existing-DID path had rev="" and root=NULL forever: any
// subsequent write (e.g. creating a post) failed, and the account was never
// announced via #identity/#sync on the firehose (only #account, later, from
// activateAccount).
func TestCreateAccountInitializesRepoForExistingDID(t *testing.T) {
	s := newTestServer(t)

	persister, err := NewDbPersister(s.db.Client(), time.Hour)
	if err != nil {
		t.Fatalf("new persister: %v", err)
	}
	s.evtman = events.NewEventManager(persister)

	// The rotation key, temporarily also the DID doc's #atproto verification
	// method — mirrors the genesis flow a self-custodying client drives (mint
	// your own did:plc naming the rotation key as both rotationKeys and the
	// #atproto signer, then swap #atproto to the PDS's key after the account
	// exists).
	k, err := atcrypto.GeneratePrivateKeyK256()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	pub, err := k.PublicKey()
	if err != nil {
		t.Fatalf("derive public key: %v", err)
	}

	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const handle = "alice.pds.test"

	cache := identity.NewMemCache(10)
	if err := cache.PutDoc(did, &identity.DidDoc{
		Id: did,
		VerificationMethods: []identity.DidDocVerificationMethod{
			{
				Id:                 did + "#atproto",
				Type:               "Multikey",
				Controller:         did,
				PublicKeyMultibase: pub.Multibase(),
			},
		},
	}); err != nil {
		t.Fatalf("seed passport cache: %v", err)
	}
	s.passport = identity.NewPassport(nil, cache)

	tok := mintServiceAuthToken(t, k.Bytes(), did, s.config.Did, "com.atproto.server.createAccount", time.Now().Add(time.Minute))

	body, err := json.Marshal(map[string]string{
		"handle":   handle,
		"email":    "alice@test.invalid",
		"password": "correct-horse-battery-staple",
		"did":      did,
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	c, rec := newRequestContext(http.MethodPost, "/xrpc/com.atproto.server.createAccount", string(body), map[string]string{
		"authorization": "Bearer " + tok,
	})

	if err := s.handleCreateAccount(c); err != nil {
		t.Fatalf("handleCreateAccount: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	urepo, err := s.getRepoActorByDid(context.Background(), did)
	if err != nil {
		t.Fatalf("getRepoActorByDid: %v", err)
	}
	if urepo.Repo.Rev == "" {
		t.Fatal("repo has no rev after account creation via the existing-DID flow")
	}
	if len(urepo.Repo.Root) == 0 {
		t.Fatal("repo has no root after account creation via the existing-DID flow")
	}

	types := eventTypesFor(t, s, did)
	for _, want := range []string{"identity", "sync"} {
		if !contains(types, want) {
			t.Fatalf("missing %q event after account creation; got %v", want, types)
		}
	}
}
