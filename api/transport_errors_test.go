package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/microservices-demo/user/users"
	stdopentracing "github.com/opentracing/opentracing-go"
)

type panicService struct {
	stubTracingService
}

func (s panicService) GetUsers(context.Context, string) ([]users.User, error) {
	panic("boom")
}

func TestRegisterInvalidJSONReturnsBadRequest(t *testing.T) {
	logger := log.NewNopLogger()
	eps := MakeEndpoints(stubTracingService{}, stdopentracing.NoopTracer{}, logger)
	handler := MakeHTTPHandler(eps, logger, stdopentracing.NoopTracer{})

	req := httptest.NewRequest(http.MethodPost, "/register", strings.NewReader(`{"username":`))
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if want, have := http.StatusBadRequest, resp.Code; want != have {
		t.Fatalf("expected status %d, got %d", want, have)
	}
	if body := resp.Body.String(); !strings.Contains(body, `"status_code":400`) {
		t.Fatalf("expected body to include 400 status code, got %s", body)
	}
}

func TestPanicInEndpointReturnsInternalServerError(t *testing.T) {
	logger := log.NewNopLogger()
	eps := MakeEndpoints(panicService{}, stdopentracing.NoopTracer{}, logger)
	handler := MakeHTTPHandler(eps, logger, stdopentracing.NoopTracer{})

	req := httptest.NewRequest(http.MethodGet, "/customers", nil)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if want, have := http.StatusInternalServerError, resp.Code; want != have {
		t.Fatalf("expected status %d, got %d", want, have)
	}
	if body := resp.Body.String(); strings.Contains(body, "boom") {
		t.Fatalf("expected panic details to be sanitized, got %s", body)
	}
}
