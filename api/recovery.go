package api

import (
	"net/http"
	"runtime/debug"

	"github.com/go-kit/kit/log"
	"github.com/gorilla/mux"
	stdopentracing "github.com/opentracing/opentracing-go"
)

func RecoverMiddleware(logger log.Logger, tracer stdopentracing.Tracer) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := setRequestMethodContext(r.Context(), r.Method)
			ctx = setRequestURIContext(ctx, r.URL.RequestURI())

			wireContext, err := tracer.Extract(
				stdopentracing.HTTPHeaders,
				stdopentracing.HTTPHeadersCarrier(r.Header),
			)
			if err == nil && hasValidIncomingSpanContext(wireContext) {
				ctx = setIncomingSpanContext(ctx, wireContext)
			}

			defer func() {
				if recovered := recover(); recovered != nil {
					traceID, spanID := traceFieldsFromContext(ctx)
					logger.Log(
						"level", "error",
						"service", "user",
						"component", "http",
						"operation", "panic_recovery",
						"traceid", traceID,
						"spanid", spanID,
						"http_method", r.Method,
						"route", r.URL.RequestURI(),
						"status_code", http.StatusInternalServerError,
						"panic", recovered,
						"stack", string(debug.Stack()),
					)
					w.Header().Set("Content-Type", "application/hal+json")
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"error":"Internal Server Error","status_code":500,"status_text":"Internal Server Error"}`))
				}
			}()

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
