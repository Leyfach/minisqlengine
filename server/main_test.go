package main

import (
    "bytes"
    "net/http"
    "net/http/httptest"
    "testing"
)

func TestHandleQuery(t *testing.T) {
    body := []byte(`{"sql":"SELECT * FROM users"}`)
    req := httptest.NewRequest("POST", "/query", bytes.NewReader(body))
    w := httptest.NewRecorder()
    handler := handleQuery(NewEngine())
    handler(w, req)

    if w.Code != http.StatusOK {
        t.Fatalf("expected 200, got %d", w.Code)
    }
}
