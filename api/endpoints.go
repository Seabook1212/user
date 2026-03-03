package api

// endpoints.go contains the endpoint definitions, including per-method request
// and response structs. Endpoints are the binding between the service and
// transport.

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/microservices-demo/user/db"
	"github.com/microservices-demo/user/users"
	stdopentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

// Endpoints collects the endpoints that comprise the Service.
type Endpoints struct {
	LoginEndpoint       endpoint.Endpoint
	RegisterEndpoint    endpoint.Endpoint
	UserGetEndpoint     endpoint.Endpoint
	UserPostEndpoint    endpoint.Endpoint
	AddressGetEndpoint  endpoint.Endpoint
	AddressPostEndpoint endpoint.Endpoint
	CardGetEndpoint     endpoint.Endpoint
	CardPostEndpoint    endpoint.Endpoint
	DeleteEndpoint      endpoint.Endpoint
	HealthEndpoint      endpoint.Endpoint
}

// MakeEndpoints returns an Endpoints structure, where each endpoint is
// backed by the given service.
func MakeEndpoints(s Service, tracer stdopentracing.Tracer, logger log.Logger) Endpoints {
	traceServer := func(operationName string) endpoint.Middleware {
		return func(next endpoint.Endpoint) endpoint.Endpoint {
			return func(ctx context.Context, request interface{}) (response interface{}, err error) {
				var span stdopentracing.Span
				if incomingSpanContext := getIncomingSpanContext(ctx); incomingSpanContext != nil {
					span = tracer.StartSpan(operationName, ext.RPCServerOption(incomingSpanContext))
				} else if parentSpan := stdopentracing.SpanFromContext(ctx); parentSpan != nil {
					span = tracer.StartSpan(operationName, ext.RPCServerOption(parentSpan.Context()))
				} else {
					span = tracer.StartSpan(operationName, ext.SpanKindRPCServer)
				}
				if requestMethod := getRequestMethodContext(ctx); requestMethod != "" {
					span.SetTag("http.method", requestMethod)
					ext.HTTPMethod.Set(span, requestMethod)
				}
				if requestURI := getRequestURIContext(ctx); requestURI != "" {
					span.SetTag("http.url", requestURI)
					ext.HTTPUrl.Set(span, requestURI)
				}
				ctx = stdopentracing.ContextWithSpan(ctx, span)
				defer func() {
					if recovered := recover(); recovered != nil {
						if state, _ := ctx.Value(requestLogStateKey{}).(*requestLogState); state != nil {
							state.endpointFailureLogged = true
						}
						stack := debug.Stack()
						traceID, spanID := traceFieldsFromContext(ctx)
						span.SetTag("error", true)
						span.SetTag("exception.type", fmt.Sprintf("%T", recovered))
						span.SetTag("exception.message", fmt.Sprint(recovered))
						span.SetTag("stack", string(stack))
						logger.Log(
							"level", "error",
							"service", "user",
							"component", "http",
							"operation", operationName,
							"traceid", traceID,
							"spanid", spanID,
							"http_method", getRequestMethodContext(ctx),
							"route", getRequestURIContext(ctx),
							"status_code", httpStatusCodeFromError(err),
							"panic", recovered,
							"stack", string(stack),
						)
						err = fmt.Errorf("panic in %s: %v", operationName, recovered)
					}
					statusCode := httpStatusCodeFromError(err)
					span.SetTag("http.status_code", statusCode)
					ext.HTTPStatusCode.Set(span, uint16(statusCode))
					if err != nil {
						span.SetTag("error", true)
						span.SetTag("error.type", classifyError(err))
						span.SetTag("error.message", err.Error())
					}
					span.Finish()
				}()
				response, err = next(ctx, request)
				return response, err
			}
		}
	}

	// Create logging middleware that extracts trace info
	loggingMiddleware := func(method string) endpoint.Middleware {
		return func(next endpoint.Endpoint) endpoint.Endpoint {
			return func(ctx context.Context, request interface{}) (interface{}, error) {
				begin := time.Now()

				traceid, spanid := traceFieldsFromContext(ctx)

				// Process the request
				response, err := next(ctx, request)
				if err != nil {
					if state, _ := ctx.Value(requestLogStateKey{}).(*requestLogState); state != nil {
						state.endpointFailureLogged = true
					}
				}

				// Build log message
				logArgs := []interface{}{
					"service", "user",
					"component", "http",
					"traceid", traceid,
					"spanid", spanid,
					"operation", method,
					"http_method", getRequestMethodContext(ctx),
					"route", getRequestURIContext(ctx),
					"status_code", httpStatusCodeFromError(err),
				}

				// Add request-specific fields based on method
				logArgs = appendRequestFields(logArgs, method, request, response, err)

				// Add error if present
				if err != nil {
					logArgs = append(logArgs,
						"level", "error",
						"error_type", classifyError(err),
						"err", err.Error(),
					)
				} else {
					logArgs = append(logArgs, "level", "info")
				}

				// Add duration
				logArgs = append(logArgs, "latency_ms", time.Since(begin).Milliseconds())

				logger.Log(logArgs...)
				return response, err
			}
		}
	}

	return Endpoints{
		LoginEndpoint:       traceServer("GET /login")(loggingMiddleware("Login")(MakeLoginEndpoint(s))),
		RegisterEndpoint:    traceServer("POST /register")(loggingMiddleware("Register")(MakeRegisterEndpoint(s))),
		HealthEndpoint:      MakeHealthEndpoint(s), // No tracing for health checks
		UserGetEndpoint:     traceServer("GET /customers")(loggingMiddleware("GetUsers")(MakeUserGetEndpoint(s))),
		UserPostEndpoint:    traceServer("POST /customers")(loggingMiddleware("PostUser")(MakeUserPostEndpoint(s))),
		AddressGetEndpoint:  traceServer("GET /addresses")(loggingMiddleware("GetAddresses")(MakeAddressGetEndpoint(s))),
		AddressPostEndpoint: traceServer("POST /addresses")(loggingMiddleware("PostAddress")(MakeAddressPostEndpoint(s))),
		CardGetEndpoint:     traceServer("GET /cards")(loggingMiddleware("GetCards")(MakeCardGetEndpoint(s))),
		DeleteEndpoint:      traceServer("DELETE /")(loggingMiddleware("Delete")(MakeDeleteEndpoint(s))),
		CardPostEndpoint:    traceServer("POST /cards")(loggingMiddleware("PostCard")(MakeCardPostEndpoint(s))),
	}
}

// appendRequestFields adds method-specific fields to log output
func appendRequestFields(logArgs []interface{}, method string, request interface{}, response interface{}, err error) []interface{} {
	switch method {
	case "GetUsers":
		req := request.(GetRequest)
		id := req.ID
		if id == "" {
			id = "all"
		}
		logArgs = append(logArgs, "id", id)
		if err == nil {
			if usersResp, ok := response.(EmbedStruct); ok {
				if ur, ok := usersResp.Embed.(usersResponse); ok {
					logArgs = append(logArgs, "result", len(ur.Users))
				}
			} else if user, ok := response.(users.User); ok {
				if user.UserID != "" {
					logArgs = append(logArgs, "result", 1)
				} else {
					logArgs = append(logArgs, "result", 0)
				}
			}
		}
	case "GetAddresses":
		req := request.(GetRequest)
		id := req.ID
		if id == "" {
			id = "all"
		}
		logArgs = append(logArgs, "id", id)
		if err == nil {
			if addrsResp, ok := response.(EmbedStruct); ok {
				if ar, ok := addrsResp.Embed.(addressesResponse); ok {
					logArgs = append(logArgs, "result", len(ar.Addresses))
				}
			}
		}
	case "GetCards":
		req := request.(GetRequest)
		id := req.ID
		if id == "" {
			id = "all"
		}
		logArgs = append(logArgs, "id", id)
		if err == nil {
			if cardsResp, ok := response.(EmbedStruct); ok {
				if cr, ok := cardsResp.Embed.(cardsResponse); ok {
					logArgs = append(logArgs, "result", len(cr.Cards))
				}
			}
		}
	case "PostUser", "PostAddress", "PostCard", "Register":
		if err == nil {
			if pr, ok := response.(postResponse); ok {
				logArgs = append(logArgs, "result", pr.ID)
			}
		}
	case "Delete":
		req := request.(deleteRequest)
		logArgs = append(logArgs, "entity", req.Entity, "id", req.ID)
		if err == nil {
			if sr, ok := response.(statusResponse); ok {
				logArgs = append(logArgs, "result", sr.Status)
			}
		}
	}
	return logArgs
}

// MakeLoginEndpoint returns an endpoint via the given service.
func MakeLoginEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(loginRequest)
		u, err := s.Login(ctx, req.Username, req.Password)
		return userResponse{User: u}, err
	}
}

// MakeRegisterEndpoint returns an endpoint via the given service.
func MakeRegisterEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(registerRequest)
		id, err := s.Register(ctx, req.Username, req.Password, req.Email, req.FirstName, req.LastName)
		return postResponse{ID: id}, err
	}
}

// MakeUserGetEndpoint returns an endpoint via the given service.
func MakeUserGetEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(GetRequest)

		usrs, err := s.GetUsers(ctx, req.ID)
		if req.ID == "" {
			return EmbedStruct{usersResponse{Users: usrs}}, err
		}
		if len(usrs) == 0 {
			if req.Attr == "addresses" {
				return EmbedStruct{addressesResponse{Addresses: make([]users.Address, 0)}}, err
			}
			if req.Attr == "cards" {
				return EmbedStruct{cardsResponse{Cards: make([]users.Card, 0)}}, err
			}
			return users.User{}, err
		}
		user := usrs[0]
		if err := db.GetUserAttributes(ctx, &user); err != nil {
			return users.User{}, fmt.Errorf("get users load attributes id=%s: %w", req.ID, err)
		}
		if req.Attr == "addresses" {
			return EmbedStruct{addressesResponse{Addresses: user.Addresses}}, err
		}
		if req.Attr == "cards" {
			return EmbedStruct{cardsResponse{Cards: user.Cards}}, err
		}
		return user, err
	}
}

// MakeUserPostEndpoint returns an endpoint via the given service.
func MakeUserPostEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(users.User)
		id, err := s.PostUser(ctx, req)
		return postResponse{ID: id}, err
	}
}

// MakeAddressGetEndpoint returns an endpoint via the given service.
func MakeAddressGetEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(GetRequest)
		adds, err := s.GetAddresses(ctx, req.ID)
		if req.ID == "" {
			return EmbedStruct{addressesResponse{Addresses: adds}}, err
		}
		if len(adds) == 0 {
			return users.Address{}, err
		}
		return adds[0], err
	}
}

// MakeAddressPostEndpoint returns an endpoint via the given service.
func MakeAddressPostEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(addressPostRequest)
		id, err := s.PostAddress(ctx, req.Address, req.UserID)
		return postResponse{ID: id}, err
	}
}

// MakeCardGetEndpoint returns an endpoint via the given service.
func MakeCardGetEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(GetRequest)
		cards, err := s.GetCards(ctx, req.ID)
		if req.ID == "" {
			return EmbedStruct{cardsResponse{Cards: cards}}, err
		}
		if len(cards) == 0 {
			return users.Card{}, err
		}
		return cards[0], err
	}
}

// MakeCardPostEndpoint returns an endpoint via the given service.
func MakeCardPostEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(cardPostRequest)
		id, err := s.PostCard(ctx, req.Card, req.UserID)
		return postResponse{ID: id}, err
	}
}

// MakeDeleteEndpoint returns an endpoint via the given service.
func MakeDeleteEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(deleteRequest)
		err = s.Delete(ctx, req.Entity, req.ID)
		if err == nil {
			return statusResponse{Status: true}, err
		}
		return statusResponse{Status: false}, err
	}
}

// MakeHealthEndpoint returns current health of the given service.
func MakeHealthEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		health := s.Health(ctx)
		return healthResponse{Health: health}, nil
	}
}

type GetRequest struct {
	ID   string
	Attr string
}

type loginRequest struct {
	Username string
	Password string
}

type userResponse struct {
	User users.User `json:"user"`
}

type usersResponse struct {
	Users []users.User `json:"customer"`
}

type addressPostRequest struct {
	users.Address
	UserID string `json:"userID"`
}

type addressesResponse struct {
	Addresses []users.Address `json:"address"`
}

type cardPostRequest struct {
	users.Card
	UserID string `json:"userID"`
}

type cardsResponse struct {
	Cards []users.Card `json:"card"`
}

type registerRequest struct {
	Username  string `json:"username"`
	Password  string `json:"password"`
	Email     string `json:"email"`
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
}

type statusResponse struct {
	Status bool `json:"status"`
}

type postResponse struct {
	ID string `json:"id"`
}

type deleteRequest struct {
	Entity string
	ID     string
}

type healthRequest struct {
	//
}

type healthResponse struct {
	Health []Health `json:"health"`
}

type EmbedStruct struct {
	Embed interface{} `json:"_embedded"`
}
