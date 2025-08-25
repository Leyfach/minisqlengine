package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestHandleQuery(t *testing.T) {
	os.Setenv("DEV_MODE", "1")
	defer os.Unsetenv("DEV_MODE")

	body := []byte(`{"sql":"SELECT * FROM users","limit":1}`)
	req := httptest.NewRequest("POST", "/query", bytes.NewReader(body))
	w := httptest.NewRecorder()
	handler := handleQuery(NewEngine())
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp QueryResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if len(resp.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(resp.Rows))
	}
}

func TestHandleQueryUnauthorized(t *testing.T) {
	os.Setenv("API_TOKEN", "secret")
	defer os.Unsetenv("API_TOKEN")

	body := []byte(`{"sql":"SELECT * FROM users"}`)
	req := httptest.NewRequest("POST", "/query", bytes.NewReader(body))
	w := httptest.NewRecorder()
	handler := handleQuery(NewEngine())
	handler(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestHandleQueryTimeout(t *testing.T) {
	os.Setenv("DEV_MODE", "1")
	defer os.Unsetenv("DEV_MODE")

	body := []byte(`{"sql":"SLEEP","timeout_ms":10}`)
	req := httptest.NewRequest("POST", "/query", bytes.NewReader(body))
	w := httptest.NewRecorder()
	handler := handleQuery(NewEngine())
	handler(w, req)

	if w.Code != http.StatusRequestTimeout {
		t.Fatalf("expected 408, got %d", w.Code)
	}
}
