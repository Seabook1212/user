package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/go-kit/kit/log"
	"github.com/microservices-demo/user/db/mongodb"
	stdopentracing "github.com/opentracing/opentracing-go"
	zipkinot "github.com/openzipkin-contrib/zipkin-go-opentracing"
	"gopkg.in/mgo.v2"
)

type requestLogState struct {
	endpointFailureLogged bool
}

type requestLogStateKey struct{}

type badRequestError struct {
	cause error
}

func (e badRequestError) Error() string {
	if e.cause == nil {
		return ErrInvalidRequest.Error()
	}
	return fmt.Sprintf("%s: %v", ErrInvalidRequest.Error(), e.cause)
}

func (e badRequestError) Unwrap() error {
	return e.cause
}

func (e badRequestError) Is(target error) bool {
	return target == ErrInvalidRequest
}

func newBadRequestError(err error) error {
	return badRequestError{cause: err}
}

func classifyError(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, ErrUnauthorized):
		return "unauthorized"
	case errors.Is(err, ErrInvalidRequest):
		return "bad_request"
	case errors.Is(err, mongodb.ErrInvalidHexID):
		return "invalid_id"
	case errors.Is(err, mgo.ErrNotFound):
		return "not_found"
	}

	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return "bad_request"
	}

	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return "bad_request"
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "bad_request"
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "timeout"
		}
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) {
			return "dns"
		}
		if errors.Is(opErr.Err, syscall.ECONNREFUSED) {
			return "connection_refused"
		}
	}

	if strings.Contains(strings.ToLower(err.Error()), "e11000") {
		return "duplicate_key"
	}

	return "internal"
}

func ClassifyError(err error) string {
	return classifyError(err)
}

func traceFieldsFromContext(ctx context.Context) (string, string) {
	if ctx == nil {
		return "", ""
	}

	if span := stdopentracing.SpanFromContext(ctx); span != nil {
		if sc, ok := span.Context().(zipkinot.SpanContext); ok {
			return fmt.Sprintf("%x", sc.TraceID.Low), fmt.Sprintf("%x", uint64(sc.ID))
		}
	}

	if sc, ok := getIncomingSpanContext(ctx).(zipkinot.SpanContext); ok {
		return fmt.Sprintf("%x", sc.TraceID.Low), fmt.Sprintf("%x", uint64(sc.ID))
	}

	return "", ""
}

func logTransportFailure(logger log.Logger, ctx context.Context, err error, extra ...interface{}) {
	traceID, spanID := traceFieldsFromContext(ctx)
	args := []interface{}{
		"level", "error",
		"service", "user",
		"component", "http",
		"traceid", traceID,
		"spanid", spanID,
		"http_method", getRequestMethodContext(ctx),
		"route", getRequestURIContext(ctx),
		"status_code", httpStatusCodeFromError(err),
		"error_type", classifyError(err),
		"err", err.Error(),
	}
	args = append(args, extra...)
	logger.Log(args...)
}
