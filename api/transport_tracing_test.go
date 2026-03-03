package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/microservices-demo/user/users"
	zipkinot "github.com/openzipkin-contrib/zipkin-go-opentracing"
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/reporter/recorder"
)

type stubTracingService struct{}

func (s stubTracingService) Login(context.Context, string, string) (users.User, error) {
	return users.New(), nil
}

func (s stubTracingService) Register(context.Context, string, string, string, string, string) (string, error) {
	return "id", nil
}

func (s stubTracingService) GetUsers(context.Context, string) ([]users.User, error) {
	return []users.User{}, nil
}

func (s stubTracingService) PostUser(context.Context, users.User) (string, error) {
	return "id", nil
}

func (s stubTracingService) GetAddresses(context.Context, string) ([]users.Address, error) {
	return []users.Address{}, nil
}

func (s stubTracingService) PostAddress(context.Context, users.Address, string) (string, error) {
	return "id", nil
}

func (s stubTracingService) GetCards(context.Context, string) ([]users.Card, error) {
	return []users.Card{}, nil
}

func (s stubTracingService) PostCard(context.Context, users.Card, string) (string, error) {
	return "id", nil
}

func (s stubTracingService) Delete(context.Context, string, string) error {
	return nil
}

func (s stubTracingService) Health(context.Context) []Health {
	return []Health{}
}

func TestHTTPHandlerEmitsServerSpanWithSplitSpans(t *testing.T) {
	rec := recorder.NewReporter()
	nativeTracer, err := zipkin.NewTracer(
		rec,
		zipkin.WithSharedSpans(false),
	)
	if err != nil {
		t.Fatalf("failed to create zipkin tracer: %v", err)
	}
	tracer := zipkinot.Wrap(nativeTracer)

	logger := log.NewNopLogger()
	eps := MakeEndpoints(stubTracingService{}, tracer, logger)
	handler := MakeHTTPHandler(eps, logger, tracer)

	expectedURLTag := "/customers?page=1&size=6&tags=blue"
	req := httptest.NewRequest(http.MethodGet, expectedURLTag, nil)
	traceIDHeader := "463ac35c9f6413ad"
	parentSpanIDHeader := "a2fb4a1d1a96d312"
	req.Header.Set("X-B3-TraceId", traceIDHeader)
	req.Header.Set("X-B3-SpanId", parentSpanIDHeader)
	req.Header.Set("X-B3-Sampled", "1")

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.Code)
	}

	parentSpanIDRaw, err := strconv.ParseUint(parentSpanIDHeader, 16, 64)
	if err != nil {
		t.Fatalf("failed to parse parent span id: %v", err)
	}
	parentSpanID := model.ID(parentSpanIDRaw)

	spans := rec.Flush()
	var serverSpan *model.SpanModel
	for i := range spans {
		if spans[i].Name == "GET /customers" {
			serverSpan = &spans[i]
			break
		}
	}
	if serverSpan == nil {
		t.Fatalf("expected GET /customers span, got %d spans", len(spans))
	}

	if want, have := traceIDHeader, serverSpan.TraceID.String(); want != have {
		t.Fatalf("expected trace id %q, got %q", want, have)
	}
	if serverSpan.Kind != model.Server {
		t.Fatalf("expected server kind, got %s", serverSpan.Kind)
	}
	if want, have := http.MethodGet, serverSpan.Tags["http.method"]; want != have {
		t.Fatalf("expected http.method tag %q, got %q", want, have)
	}
	if want, have := expectedURLTag, serverSpan.Tags["http.url"]; want != have {
		t.Fatalf("expected http.url tag %q, got %q", want, have)
	}
	if want, have := "200", serverSpan.Tags["http.status_code"]; want != have {
		t.Fatalf("expected http.status_code tag %q, got %q", want, have)
	}
	if serverSpan.ParentID == nil {
		t.Fatalf("expected parent span id to be set")
	}
	if want, have := parentSpanID, *serverSpan.ParentID; want != have {
		t.Fatalf("expected parent span id %s, got %s", want.String(), have.String())
	}
	if serverSpan.ID == parentSpanID {
		t.Fatalf("expected split span id, got same as incoming parent span id %s", parentSpanID.String())
	}
}
