package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/events"
	"github.com/haileyok/cocoon/identity"
	"github.com/haileyok/cocoon/plc"
)

// fakePlcDirectory serves a minimal plc.directory double: a GET audit-log
// endpoint returning a single, caller-supplied operation, and a POST
// operation-submission endpoint that just records whether it was hit. Real
// plc.directory validates submitted signatures; this double doesn't need to,
// since handleIdentityUpdateHandle's own logic (not plc.directory's) is what
// these tests are checking.
func fakePlcDirectory(t *testing.T, did string, publishedAka []string) (*httptest.Server, *bool) {
	t.Helper()
	sawSubmit := false

	mux := http.NewServeMux()
	mux.HandleFunc("GET /"+did+"/log/audit", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		fmt.Fprintf(w, `[{
			"did": %q,
			"cid": "bafytest",
			"nullified": false,
			"createdAt": "2024-01-01T00:00:00Z",
			"operation": {
				"sig": "test",
				"prev": null,
				"type": "plc_operation",
				"alsoKnownAs": %s,
				"rotationKeys": ["did:key:test"],
				"verificationMethods": {"atproto": "did:key:test"},
				"services": {"atproto_pds": {"type": "AtprotoPersonalDataServer", "endpoint": "https://pds.test"}}
			}
		}]`, did, jsonStringArray(publishedAka))
	})
	mux.HandleFunc("POST /"+did, func(w http.ResponseWriter, r *http.Request) {
		sawSubmit = true
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &sawSubmit
}

func jsonStringArray(vs []string) string {
	out := "["
	for i, v := range vs {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf("%q", v)
	}
	return out + "]"
}

// wireUpdateHandleServer fills in the collaborators handleIdentityUpdateHandle
// needs beyond what newTestServer already sets up: a plcClient pointed at a
// local fake plc.directory (rather than the real one), a passport for the
// post-update doc-cache bust, and an event manager for the #identity event.
func wireUpdateHandleServer(t *testing.T, s *Server, did string, publishedAka []string) *bool {
	t.Helper()

	fakePlc, sawSubmit := fakePlcDirectory(t, did, publishedAka)

	rotationKey, err := atcrypto.GeneratePrivateKeyK256()
	if err != nil {
		t.Fatalf("generate rotation key: %v", err)
	}

	plcClient, err := plc.NewClient(&plc.ClientArgs{
		H:           fakePlc.Client(),
		Service:     fakePlc.URL,
		RotationKey: rotationKey.Bytes(),
		PdsHostname: testHostname,
	})
	if err != nil {
		t.Fatalf("new plc client: %v", err)
	}
	s.plcClient = plcClient
	s.passport = identity.NewPassport(nil, identity.NewMemCache(10))

	persister, err := NewDbPersister(s.db.Client(), time.Hour)
	if err != nil {
		t.Fatalf("new persister: %v", err)
	}
	s.evtman = events.NewEventManager(persister)

	return sawSubmit
}

// TestUpdateHandleSkipsSelfSignWhenAlreadyPublished covers the fix: an
// account whose DID document already shows the requested handle (e.g. a
// self-custodied account that published the change directly, holding its
// own rotation key rather than this PDS's) should succeed without this PDS
// attempting to sign a redundant - and for a self-custodied account,
// cryptographically impossible - PLC operation of its own.
func TestUpdateHandleSkipsSelfSignWhenAlreadyPublished(t *testing.T) {
	s := newTestServer(t)
	ra := repoActorFor(t, s, "alice.pds.test")
	newHandle := "alice2.pds.test"

	sawSubmit := wireUpdateHandleServer(t, s, ra.Repo.Did, []string{"at://" + newHandle})

	body := `{"handle":"` + newHandle + `"}`
	c, rec := newRequestContext(http.MethodPost, "/xrpc/com.atproto.identity.updateHandle", body, nil)
	c.Set("repo", ra)

	if err := s.handleIdentityUpdateHandle(c); err != nil {
		t.Fatalf("handleIdentityUpdateHandle: %v", err)
	}
	if rec.Code != 0 && rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if *sawSubmit {
		t.Fatal("expected no PLC operation submission - the handle was already published")
	}

	var dbHandle string
	if err := s.db.Client().Raw("SELECT handle FROM actors WHERE did = ?", ra.Repo.Did).Scan(&dbHandle).Error; err != nil {
		t.Fatalf("query actors: %v", err)
	}
	if dbHandle != newHandle {
		t.Fatalf("handle in db = %q, want %q", dbHandle, newHandle)
	}

	types := eventTypesFor(t, s, ra.Repo.Did)
	if !contains(types, "identity") {
		t.Fatalf("missing identity event after handle update; got %v", types)
	}
}

// TestUpdateHandleStillSubmitsWhenNotAlreadyPublished confirms the fast path
// is conditional, not a blanket bypass: when the DID document does not
// already show the requested handle, the PDS still attempts to self-sign
// and submit a PLC operation, exactly as before this fix.
func TestUpdateHandleStillSubmitsWhenNotAlreadyPublished(t *testing.T) {
	s := newTestServer(t)
	ra := repoActorFor(t, s, "bob.pds.test")
	newHandle := "bob2.pds.test"

	// Audit log shows the *old* handle still - nothing published yet.
	sawSubmit := wireUpdateHandleServer(t, s, ra.Repo.Did, []string{"at://" + ra.Handle})

	body := `{"handle":"` + newHandle + `"}`
	c, _ := newRequestContext(http.MethodPost, "/xrpc/com.atproto.identity.updateHandle", body, nil)
	c.Set("repo", ra)

	// The fake plc.directory accepts any POST unconditionally (it isn't
	// re-validating signatures), so this should proceed all the way
	// through rather than erroring - the point of this test is only that
	// the submission was *attempted*.
	if err := s.handleIdentityUpdateHandle(c); err != nil {
		t.Fatalf("handleIdentityUpdateHandle: %v", err)
	}
	if !*sawSubmit {
		t.Fatal("expected a PLC operation submission - the handle was not already published")
	}
}
