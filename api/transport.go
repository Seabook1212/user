package api

// transport.go contains the binding from endpoints to a concrete transport.
// In our case we just use a REST-y HTTP transport.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/go-kit/kit/log"
	httptransport "github.com/go-kit/kit/transport/http"
	"github.com/gorilla/mux"
	"github.com/microservices-demo/user/db/mongodb"
	"github.com/microservices-demo/user/users"
	stdopentracing "github.com/opentracing/opentracing-go"
	zipkinot "github.com/openzipkin-contrib/zipkin-go-opentracing"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/mgo.v2"
)

var (
	ErrInvalidRequest = errors.New("Invalid request")
)

type incomingSpanContextKey struct{}
type requestURIContextKey struct{}
type requestMethodContextKey struct{}

func setIncomingSpanContext(ctx context.Context, sc stdopentracing.SpanContext) context.Context {
	if sc == nil {
		return ctx
	}
	return context.WithValue(ctx, incomingSpanContextKey{}, sc)
}

func getIncomingSpanContext(ctx context.Context) stdopentracing.SpanContext {
	sc, _ := ctx.Value(incomingSpanContextKey{}).(stdopentracing.SpanContext)
	return sc
}

func setRequestURIContext(ctx context.Context, requestURI string) context.Context {
	if strings.TrimSpace(requestURI) == "" {
		return ctx
	}
	return context.WithValue(ctx, requestURIContextKey{}, requestURI)
}

func getRequestURIContext(ctx context.Context) string {
	uri, _ := ctx.Value(requestURIContextKey{}).(string)
	return uri
}

func setRequestMethodContext(ctx context.Context, method string) context.Context {
	if strings.TrimSpace(method) == "" {
		return ctx
	}
	return context.WithValue(ctx, requestMethodContextKey{}, method)
}

func getRequestMethodContext(ctx context.Context) string {
	method, _ := ctx.Value(requestMethodContextKey{}).(string)
	return method
}

func hasValidIncomingSpanContext(sc stdopentracing.SpanContext) bool {
	if sc == nil {
		return false
	}
	// zipkin-go-opentracing can return a zero-value span context when no B3 trace
	// headers are present; treat that as "not found".
	if zsc, ok := sc.(zipkinot.SpanContext); ok {
		return !zsc.TraceID.Empty() && zsc.ID > 0
	}
	return true
}

// MakeHTTPHandler mounts the endpoints into a REST-y HTTP handler.
func MakeHTTPHandler(e Endpoints, logger log.Logger, tracer stdopentracing.Tracer) *mux.Router {
	r := mux.NewRouter().StrictSlash(false)
	r.Use(RecoverMiddleware(logger, tracer))
	options := []httptransport.ServerOption{
		httptransport.ServerErrorEncoder(makeErrorEncoder(logger)),
		httptransport.ServerBefore(func(ctx context.Context, req *http.Request) context.Context {
			ctx = setRequestURIContext(ctx, req.URL.RequestURI())
			ctx = setRequestMethodContext(ctx, req.Method)
			ctx = context.WithValue(ctx, requestLogStateKey{}, &requestLogState{})

			wireContext, err := tracer.Extract(
				stdopentracing.HTTPHeaders,
				stdopentracing.HTTPHeadersCarrier(req.Header),
			)
			if err != nil {
				if err != stdopentracing.ErrSpanContextNotFound {
					logTransportFailure(logger, ctx, err, "operation", "extract_trace_context")
				}
				return ctx
			}
			if !hasValidIncomingSpanContext(wireContext) {
				return ctx
			}
			return setIncomingSpanContext(ctx, wireContext)
		}),
	}

	// Options for health/metrics endpoints without tracing
	healthOptions := []httptransport.ServerOption{
		httptransport.ServerErrorEncoder(makeErrorEncoder(logger)),
	}

	// GET /login       Login
	// GET /register    Register
	// GET /health      Health Check

	r.Methods("GET").Path("/login").Handler(httptransport.NewServer(
		e.LoginEndpoint,
		decodeLoginRequest,
		encodeResponse,
		options...,
	))
	r.Methods("POST").Path("/register").Handler(httptransport.NewServer(
		e.RegisterEndpoint,
		decodeRegisterRequest,
		encodeResponse,
		options...,
	))
	r.Methods("GET").PathPrefix("/customers").Handler(httptransport.NewServer(
		e.UserGetEndpoint,
		decodeGetRequest,
		encodeResponse,
		options...,
	))
	r.Methods("GET").PathPrefix("/cards").Handler(httptransport.NewServer(
		e.CardGetEndpoint,
		decodeGetRequest,
		encodeResponse,
		options...,
	))
	r.Methods("GET").PathPrefix("/addresses").Handler(httptransport.NewServer(
		e.AddressGetEndpoint,
		decodeGetRequest,
		encodeResponse,
		options...,
	))
	r.Methods("POST").Path("/customers").Handler(httptransport.NewServer(
		e.UserPostEndpoint,
		decodeUserRequest,
		encodeResponse,
		options...,
	))
	r.Methods("POST").Path("/addresses").Handler(httptransport.NewServer(
		e.AddressPostEndpoint,
		decodeAddressRequest,
		encodeResponse,
		options...,
	))
	r.Methods("POST").Path("/cards").Handler(httptransport.NewServer(
		e.CardPostEndpoint,
		decodeCardRequest,
		encodeResponse,
		options...,
	))
	r.Methods("DELETE").PathPrefix("/").Handler(httptransport.NewServer(
		e.DeleteEndpoint,
		decodeDeleteRequest,
		encodeResponse,
		options...,
	))
	r.Methods("GET").PathPrefix("/health").Handler(httptransport.NewServer(
		e.HealthEndpoint,
		decodeHealthRequest,
		encodeHealthResponse,
		healthOptions...,
	))
	r.Handle("/metrics", promhttp.Handler())
	return r
}

func makeErrorEncoder(logger log.Logger) httptransport.ErrorEncoder {
	return func(ctx context.Context, err error, w http.ResponseWriter) {
		if state, _ := ctx.Value(requestLogStateKey{}).(*requestLogState); state == nil || !state.endpointFailureLogged {
			logTransportFailure(logger, ctx, err, "operation", "transport")
		}

		code := httpStatusCodeFromError(err)
		w.Header().Set("Content-Type", "application/hal+json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error":       publicErrorMessage(err),
			"status_code": code,
			"status_text": http.StatusText(code),
		})
	}
}

func encodeError(_ context.Context, err error, w http.ResponseWriter) {
	code := httpStatusCodeFromError(err)
	w.Header().Set("Content-Type", "application/hal+json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"error":       publicErrorMessage(err),
		"status_code": code,
		"status_text": http.StatusText(code),
	})
}

func httpStatusCodeFromError(err error) int {
	if err == nil {
		return http.StatusOK
	}
	switch {
	case errors.Is(err, ErrUnauthorized):
		return http.StatusUnauthorized
	case errors.Is(err, ErrInvalidRequest), errors.Is(err, mongodb.ErrInvalidHexID):
		return http.StatusBadRequest
	case errors.Is(err, mgo.ErrNotFound):
		return http.StatusNotFound
	case classifyError(err) == "duplicate_key":
		return http.StatusConflict
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

func decodeLoginRequest(_ context.Context, r *http.Request) (interface{}, error) {
	u, p, ok := r.BasicAuth()
	if !ok {
		return loginRequest{}, ErrUnauthorized
	}

	return loginRequest{
		Username: u,
		Password: p,
	}, nil
}

func decodeRegisterRequest(_ context.Context, r *http.Request) (interface{}, error) {
	defer r.Body.Close()
	reg := registerRequest{}
	err := json.NewDecoder(r.Body).Decode(&reg)
	if err != nil {
		return nil, newBadRequestError(err)
	}
	return reg, nil
}

func decodeDeleteRequest(_ context.Context, r *http.Request) (interface{}, error) {
	d := deleteRequest{}
	u := strings.Split(r.URL.Path, "/")
	if len(u) == 3 {
		d.Entity = u[1]
		d.ID = u[2]
		return d, nil
	}
	return d, newBadRequestError(nil)
}

func decodeGetRequest(_ context.Context, r *http.Request) (interface{}, error) {
	g := GetRequest{}
	u := strings.Split(r.URL.Path, "/")
	if len(u) > 2 {
		g.ID = u[2]
		if len(u) > 3 {
			g.Attr = u[3]
		}
	}
	return g, nil
}

func decodeUserRequest(_ context.Context, r *http.Request) (interface{}, error) {
	defer r.Body.Close()
	u := users.User{}
	err := json.NewDecoder(r.Body).Decode(&u)
	if err != nil {
		return nil, newBadRequestError(err)
	}
	return u, nil
}

func decodeAddressRequest(_ context.Context, r *http.Request) (interface{}, error) {
	defer r.Body.Close()
	a := addressPostRequest{}
	err := json.NewDecoder(r.Body).Decode(&a)
	if err != nil {
		return nil, newBadRequestError(err)
	}
	return a, nil
}

func decodeCardRequest(_ context.Context, r *http.Request) (interface{}, error) {
	defer r.Body.Close()
	c := cardPostRequest{}
	err := json.NewDecoder(r.Body).Decode(&c)
	if err != nil {
		return nil, newBadRequestError(err)
	}
	return c, nil
}

func decodeHealthRequest(_ context.Context, r *http.Request) (interface{}, error) {
	return struct{}{}, nil
}

func encodeHealthResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	return encodeResponse(ctx, w, response.(healthResponse))
}

func encodeResponse(_ context.Context, w http.ResponseWriter, response interface{}) error {
	// All of our response objects are JSON serializable, so we just do that.
	w.Header().Set("Content-Type", "application/hal+json")
	return json.NewEncoder(w).Encode(response)
}

func publicErrorMessage(err error) string {
	switch httpStatusCodeFromError(err) {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusNotFound, http.StatusConflict:
		return err.Error()
	default:
		return http.StatusText(http.StatusInternalServerError)
	}
}
