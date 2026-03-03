package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	corelog "log"

	"github.com/go-kit/kit/log"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	"github.com/microservices-demo/user/api"
	"github.com/microservices-demo/user/db"
	"github.com/microservices-demo/user/db/mongodb"
	stdopentracing "github.com/opentracing/opentracing-go"
	zipkinot "github.com/openzipkin-contrib/zipkin-go-opentracing"
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/reporter"
	zipkinhttp "github.com/openzipkin/zipkin-go/reporter/http"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	commonMiddleware "github.com/weaveworks/common/middleware"
)

var (
	port string
	zip  string
)

var kubernetesTraceTagEnvVars = map[string]string{
	"container": "CONTAINER_NAME",
	"pod":       "POD_NAME",
	"namespace": "POD_NAMESPACE",
	"node":      "NODE_NAME",
}

var (
	HTTPLatency = stdprometheus.NewHistogramVec(stdprometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "Time (in seconds) spent serving HTTP requests.",
		Buckets: stdprometheus.DefBuckets,
	}, []string{"method", "route", "status_code", "ws"})

	HTTPRequestsInFlight = stdprometheus.NewGaugeVec(stdprometheus.GaugeOpts{
		Name: "http_requests_in_flight",
		Help: "Current number of HTTP requests being served.",
	}, []string{"method", "route"})

	HTTPRequestBodySize = stdprometheus.NewHistogramVec(stdprometheus.HistogramOpts{
		Name:    "http_request_size_bytes",
		Help:    "Size of HTTP request bodies.",
		Buckets: stdprometheus.ExponentialBuckets(1024, 2, 10),
	}, []string{"method", "route"})

	HTTPResponseBodySize = stdprometheus.NewHistogramVec(stdprometheus.HistogramOpts{
		Name:    "http_response_size_bytes",
		Help:    "Size of HTTP response bodies.",
		Buckets: stdprometheus.ExponentialBuckets(1024, 2, 10),
	}, []string{"method", "route"})
)

const (
	ServiceName          = "user"
	defaultZipkinHost    = "jaeger-collector.observability.svc.cluster.local"
	defaultZipkinPort    = "9411"
	defaultZipkinBaseURL = "http://jaeger-collector.observability.svc.cluster.local:9411"
)

func init() {
	stdprometheus.MustRegister(HTTPLatency)
	stdprometheus.MustRegister(HTTPRequestsInFlight)
	stdprometheus.MustRegister(HTTPRequestBodySize)
	stdprometheus.MustRegister(HTTPResponseBodySize)
	flag.StringVar(&zip, "zipkin", os.Getenv("ZIPKIN"), "Zipkin address")
	flag.StringVar(&port, "port", "8084", "Port on which to run")
	db.Register("mongodb", &mongodb.Mongo{})
}

func main() {

	flag.Parse()
	// Mechanical stuff.
	errc := make(chan error)

	// Log domain.
	var logger log.Logger
	{
		logger = log.NewLogfmtLogger(os.Stderr)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC)
		logger = log.With(logger, "caller", log.DefaultCaller)
	}

	zipkinAddr, zipkinSource := resolveZipkinEndpoint()
	logger.Log(
		"msg", "Tracing configuration resolved",
		"zipkin_addr", zipkinAddr,
		"zipkin_source", zipkinSource,
		"zipkin_flag", strings.TrimSpace(zip),
		"zipkin_env", strings.TrimSpace(os.Getenv("ZIPKIN")),
		"zipkin_host_env", strings.TrimSpace(os.Getenv("ZIPKIN_HOST")),
		"zipkin_port_env", strings.TrimSpace(os.Getenv("ZIPKIN_PORT")),
		"zipkin_base_url_env", strings.TrimSpace(os.Getenv("ZIPKIN_BASE_URL")),
		"args", strings.Join(os.Args[1:], " "),
	)

	// Find service local IP.
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		logger.Log("err", err)
		os.Exit(1)
	}
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	host := strings.Split(localAddr.String(), ":")[0]
	defer conn.Close()

	var tracer stdopentracing.Tracer
	var zipkinReporter reporter.Reporter
	{
		if zipkinAddr == "" {
			tracer = stdopentracing.NoopTracer{}
			logger.Log(
				"msg", "Tracing disabled - no Zipkin endpoint configured",
				"checked_envs", "ZIPKIN,JAEGER_COLLECTOR_URL,JAEGER_ENDPOINT",
			)
		} else {
			logger := log.With(logger, "tracer", "Zipkin")
			logger.Log("addr", zipkinAddr, "source", zipkinSource)

			// Create a standard logger for Zipkin reporter errors
			zipkinLogger := corelog.New(os.Stderr, "ZIPKIN: ", corelog.LstdFlags)

			// Create reporter with batching for better performance
			zipkinReporter = zipkinhttp.NewReporter(
				zipkinAddr,
				zipkinhttp.BatchSize(100),
				zipkinhttp.BatchInterval(1*time.Second),
				zipkinhttp.Logger(zipkinLogger),
			)

			endpoint, err := zipkin.NewEndpoint(ServiceName, fmt.Sprintf("%v:%v", host, port))
			if err != nil {
				logger.Log("err", err)
				os.Exit(1)
			}

			tracerOptions := []zipkin.TracerOption{
				zipkin.WithLocalEndpoint(endpoint),
				// Create dedicated server span IDs instead of reusing caller span IDs.
				zipkin.WithSharedSpans(false),
			}
			if kubernetesTraceTags := getKubernetesTraceTags(); len(kubernetesTraceTags) > 0 {
				logger.Log("msg", "Zipkin default tags enabled", "tags", fmt.Sprintf("%v", kubernetesTraceTags))
				tracerOptions = append(tracerOptions, zipkin.WithTags(kubernetesTraceTags))
			}

			nativeTracer, err := zipkin.NewTracer(zipkinReporter, tracerOptions...)
			if err != nil {
				logger.Log("err", err)
				os.Exit(1)
			}

			tracer = zipkinot.Wrap(nativeTracer)
			logger.Log("msg", "Zipkin tracer initialized successfully")
		}
		stdopentracing.InitGlobalTracer(tracer)
	}

	// Ensure reporter is closed on shutdown to flush pending spans
	defer func() {
		if zipkinReporter != nil {
			zipkinReporter.Close()
		}
	}()
	const dbRetryBackoff = 1 * time.Second
	for attempt := 1; ; attempt++ {
		err := db.Init()
		if err != nil {
			if err == db.ErrNoDatabaseSelected {
				logger.Log(
					"level", "error",
					"service", ServiceName,
					"component", "startup",
					"dependency", "mongodb",
					"operation", "init",
					"retry_attempt", attempt,
					"error_type", "configuration",
					"err", err,
				)
				os.Exit(1)
			}
			logger.Log(
				"level", "error",
				"service", ServiceName,
				"component", "startup",
				"dependency", "mongodb",
				"target", strings.TrimSpace(os.Getenv("MONGO_HOST")),
				"operation", "init",
				"retry_attempt", attempt,
				"retry_in", dbRetryBackoff.String(),
				"error_type", api.ClassifyError(err),
				"err", err,
			)
			time.Sleep(dbRetryBackoff)
			continue
		}
		break
	}

	fieldKeys := []string{"method"}
	// Service domain.
	var service api.Service
	{
		service = api.NewFixedService()
		// Logging now done at endpoint level with trace information
		// service = api.LoggingMiddleware(logger)(service)
		service = api.NewInstrumentingService(
			kitprometheus.NewCounterFrom(
				stdprometheus.CounterOpts{
					Namespace: "microservices_demo",
					Subsystem: "user",
					Name:      "request_count",
					Help:      "Number of requests received.",
				},
				fieldKeys),
			kitprometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
				Namespace: "microservices_demo",
				Subsystem: "user",
				Name:      "request_latency_microseconds",
				Help:      "Total duration of requests in microseconds.",
			}, fieldKeys),
			service,
		)
	}

	// Endpoint domain.
	endpoints := api.MakeEndpoints(service, tracer, logger)

	// HTTP router
	router := api.MakeHTTPHandler(endpoints, logger, tracer)

	httpMiddleware := []commonMiddleware.Interface{
		commonMiddleware.Instrument{
			Duration:         HTTPLatency,
			InflightRequests: HTTPRequestsInFlight,
			RequestBodySize:  HTTPRequestBodySize,
			ResponseBodySize: HTTPResponseBodySize,
			RouteMatcher:     router,
		},
	}

	// Handler
	handler := commonMiddleware.Merge(httpMiddleware...).Wrap(router)

	// Create and launch the HTTP server.
	server := &http.Server{
		Addr:              fmt.Sprintf(":%v", port),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Log(
			"transport", "HTTP",
			"port", port,
			"read_header_timeout", server.ReadHeaderTimeout.String(),
			"read_timeout", server.ReadTimeout.String(),
			"write_timeout", server.WriteTimeout.String(),
			"idle_timeout", server.IdleTimeout.String(),
		)
		errc <- server.ListenAndServe()
	}()

	// Capture interrupts.
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errc <- fmt.Errorf("%s", <-c)
	}()

	if err := <-errc; err != nil {
		logger.Log("level", "error", "service", ServiceName, "component", "runtime", "exit", err)
	}
}

func getKubernetesTraceTags() map[string]string {
	tags := make(map[string]string, len(kubernetesTraceTagEnvVars))

	for tagName, envVarName := range kubernetesTraceTagEnvVars {
		if value := strings.TrimSpace(os.Getenv(envVarName)); value != "" {
			tags[tagName] = value
		}
	}

	return tags
}

func resolveZipkinEndpoint() (string, string) {
	if value := strings.TrimSpace(zip); value != "" {
		return normalizeZipkinCollectorURL(value), "flag(-zipkin)"
	}
	if value := strings.TrimSpace(os.Getenv("ZIPKIN")); value != "" {
		return normalizeZipkinCollectorURL(value), "env(ZIPKIN)"
	}
	if value := strings.TrimSpace(os.Getenv("JAEGER_COLLECTOR_URL")); value != "" {
		return normalizeZipkinCollectorURL(value), "env(JAEGER_COLLECTOR_URL)"
	}
	if value := strings.TrimSpace(os.Getenv("JAEGER_ENDPOINT")); value != "" {
		return normalizeZipkinCollectorURL(value), "env(JAEGER_ENDPOINT)"
	}

	zipkinBaseURL := strings.TrimSpace(os.Getenv("ZIPKIN_BASE_URL"))
	if zipkinBaseURL != "" {
		return normalizeZipkinCollectorURL(zipkinBaseURL), "env(ZIPKIN_BASE_URL)"
	}

	zipkinHost := strings.TrimSpace(os.Getenv("ZIPKIN_HOST"))
	if zipkinHost == "" {
		zipkinHost = defaultZipkinHost
	}
	zipkinPort := strings.TrimSpace(os.Getenv("ZIPKIN_PORT"))
	if zipkinPort == "" {
		zipkinPort = defaultZipkinPort
	}

	if zipkinHost != "" && zipkinPort != "" {
		return normalizeZipkinCollectorURL(fmt.Sprintf("http://%s:%s", zipkinHost, zipkinPort)), "env(ZIPKIN_HOST/ZIPKIN_PORT)"
	}

	return normalizeZipkinCollectorURL(defaultZipkinBaseURL), "default(ZIPKIN_BASE_URL)"
}

func normalizeZipkinCollectorURL(value string) string {
	url := strings.TrimSpace(value)
	if url == "" {
		return ""
	}
	if strings.HasSuffix(strings.ToLower(url), "/api/v2/spans") {
		return url
	}
	return strings.TrimRight(url, "/") + "/api/v2/spans"
}
