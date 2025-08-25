package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"time"
)

// QueryRequest defines the HTTP body for a SQL query.
// Optional pagination and timeout controls are provided via
// limit/offset and timeout_ms respectively.
type QueryRequest struct {
	SQL       string `json:"sql"`
	Limit     int    `json:"limit,omitempty"`
	Offset    int    `json:"offset,omitempty"`
	TimeoutMS int    `json:"timeout_ms,omitempty"`
}

// APIError represents a structured error in the JSON contract.
type APIError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// QueryResponse is returned by the engine and always follows the
// {columns, rows, error} schema.
type QueryResponse struct {
	Columns []string        `json:"columns,omitempty"`
	Rows    [][]interface{} `json:"rows,omitempty"`
	Error   *APIError       `json:"error,omitempty"`
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

// Query executes SQL with basic limit/offset handling.
// If sql is empty an error is returned. A special SQL of "SLEEP"
// simulates a slow query for timeout testing.
func (e *Engine) Query(sql string, limit, offset int) (QueryResponse, error) {
	if sql == "" {
		return QueryResponse{}, errors.New("empty SQL")
	}
	if sql == "SLEEP" {
		time.Sleep(200 * time.Millisecond)
	}
	rows := e.rows
	if offset > 0 {
		if offset >= len(rows) {
			rows = [][]interface{}{}
		} else {
			rows = rows[offset:]
		}
	}
	if limit > 0 && limit < len(rows) {
		rows = rows[:limit]
	}
	return QueryResponse{Columns: e.columns, Rows: rows}, nil
}

func handleQuery(e *Engine) http.HandlerFunc {
	token := os.Getenv("API_TOKEN")
	devMode := os.Getenv("DEV_MODE") == "1"
	return func(w http.ResponseWriter, r *http.Request) {
		// Authorization
		if !devMode && token != "" {
			auth := r.Header.Get("Authorization")
			if auth != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				json.NewEncoder(w).Encode(QueryResponse{Error: &APIError{Code: http.StatusUnauthorized, Message: "unauthorized"}})
				return
			}
		}

		var req QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(QueryResponse{Error: &APIError{Code: http.StatusBadRequest, Message: err.Error()}})
			return
		}

		// Audit log
		log.Printf("query: %s", req.SQL)

		timeout := time.Duration(req.TimeoutMS) * time.Millisecond
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		resultCh := make(chan QueryResponse, 1)
		errCh := make(chan error, 1)
		go func() {
			resp, err := e.Query(req.SQL, req.Limit, req.Offset)
			if err != nil {
				errCh <- err
				return
			}
			resultCh <- resp
		}()

		select {
		case <-ctx.Done():
			w.WriteHeader(http.StatusRequestTimeout)
			json.NewEncoder(w).Encode(QueryResponse{Error: &APIError{Code: http.StatusRequestTimeout, Message: "timeout"}})
		case err := <-errCh:
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(QueryResponse{Error: &APIError{Code: http.StatusBadRequest, Message: err.Error()}})
		case resp := <-resultCh:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(resp)
		}
	}
}

func main() {
	engine := NewEngine()
	http.HandleFunc("/query", handleQuery(engine))
	http.ListenAndServe(":8080", nil)
}
