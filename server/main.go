package main

import (
    "encoding/json"
    "net/http"

    "github.com/go-chi/chi/v5"
)

type QueryRequest struct {
    SQL string `json:"sql"`
}

type QueryResponse struct {
    Columns []string        `json:"columns"`
    Rows    [][]interface{} `json:"rows"`
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
    var req QueryRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    resp := QueryResponse{
        Columns: []string{"id", "name"},
        Rows:    [][]interface{}{{1, "Alice"}},
    }
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(resp)
}

func main() {
    r := chi.NewRouter()
    r.Post("/query", handleQuery)
    http.ListenAndServe(":8080", r)
}
