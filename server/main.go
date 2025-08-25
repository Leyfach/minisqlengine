package main

import (
    "encoding/json"
    "net/http"
)

type QueryRequest struct {
    SQL string `json:"sql"`
}

type QueryResponse struct {
    Columns []string        `json:"columns"`
    Rows    [][]interface{} `json:"rows"`
}

type Engine struct {
    columns []string
    rows    [][]interface{}
}

func NewEngine() *Engine {
    return &Engine{
        columns: []string{"id", "name"},
        rows:    [][]interface{}{{1, "Alice"}},
    }
}

func (e *Engine) Query(sql string) QueryResponse {
    // In a real implementation, parse and execute the SQL.
    return QueryResponse{Columns: e.columns, Rows: e.rows}
}

func handleQuery(e *Engine) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        var req QueryRequest
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            http.Error(w, err.Error(), http.StatusBadRequest)
            return
        }
        resp := e.Query(req.SQL)
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(resp)
    }
}

func main() {
    engine := NewEngine()
    http.HandleFunc("/query", handleQuery(engine))
    http.ListenAndServe(":8080", nil)
}

