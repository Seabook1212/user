package api

import (
	"testing"

	stdopentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	zipkinot "github.com/openzipkin-contrib/zipkin-go-opentracing"
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/reporter/recorder"
)

func TestZipkinBridgePreservesSpanKindAndPeerServiceTags(t *testing.T) {
	rec := recorder.NewReporter()
	nativeTracer, err := zipkin.NewTracer(rec)
	if err != nil {
		t.Fatalf("failed to create zipkin tracer: %v", err)
	}
	tracer := zipkinot.Wrap(nativeTracer)

	srvOTSpan := tracer.StartSpan("GET /customers", ext.SpanKindRPCServer)
	srvOTSpan.Finish()

	clientOTSpan := tracer.StartSpan(
		"mongodb: find all users",
		ext.SpanKindRPCClient,
		stdopentracing.Tag{Key: string(ext.PeerService), Value: "user-db"},
	)
	clientOTSpan.Finish()

	spans := rec.Flush()
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}

	var serverSpanModel model.SpanModel
	var clientSpanModel model.SpanModel
	for _, span := range spans {
		switch span.Name {
		case "GET /customers":
			serverSpanModel = span
		case "mongodb: find all users":
			clientSpanModel = span
		}
	}

	if serverSpanModel.Name == "" {
		t.Fatalf("server span not found")
	}
	if clientSpanModel.Name == "" {
		t.Fatalf("client span not found")
	}

	if want, have := model.Server, serverSpanModel.Kind; want != have {
		t.Fatalf("expected server kind, got %s", have)
	}
	if want, have := "server", serverSpanModel.Tags["span.kind"]; want != have {
		t.Fatalf("expected span.kind tag %q, got %q", want, have)
	}

	if want, have := model.Client, clientSpanModel.Kind; want != have {
		t.Fatalf("expected client kind, got %s", have)
	}
	if want, have := "client", clientSpanModel.Tags["span.kind"]; want != have {
		t.Fatalf("expected span.kind tag %q, got %q", want, have)
	}
	if want, have := "user-db", clientSpanModel.Tags["peer.service"]; want != have {
		t.Fatalf("expected peer.service tag %q, got %q", want, have)
	}
}
