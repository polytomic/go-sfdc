package composite

import "net/http"

type MockLimiter struct{}

func (m *MockLimiter) Client(h *http.Client) *http.Client { return h }
