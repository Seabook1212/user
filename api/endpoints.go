package api

// endpoints.go contains the endpoint definitions, including per-method request
// and response structs. Endpoints are the binding between the service and
// transport.

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/tracing/opentracing"
	"github.com/microservices-demo/user/db"
	"github.com/microservices-demo/user/users"
	stdopentracing "github.com/opentracing/opentracing-go"
	zipkinot "github.com/openzipkin-contrib/zipkin-go-opentracing"
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
	// Create logging middleware that extracts trace info
	loggingMiddleware := func(method string) endpoint.Middleware {
		return func(next endpoint.Endpoint) endpoint.Endpoint {
			return func(ctx context.Context, request interface{}) (interface{}, error) {
				begin := time.Now()
				response, err := next(ctx, request)

				// Extract trace information from context
				span := stdopentracing.SpanFromContext(ctx)
				traceid := ""
				spanid := ""
				if span != nil {
					if sc, ok := span.Context().(zipkinot.SpanContext); ok {
						// Format trace ID - use Low part for 64-bit trace IDs
						traceid = fmt.Sprintf("%x", sc.TraceID.Low)
						// Format span ID - this is the server span ID created by TraceServer
						spanid = fmt.Sprintf("%x", uint64(sc.ID))
					}
				}

				// Build log message
				logArgs := []interface{}{
					"traceid", traceid,
					"spanid", spanid,
					"method", method,
				}

				// Add request-specific fields based on method
				logArgs = appendRequestFields(logArgs, method, request, response, err)

				// Add error if present
				if err != nil {
					logArgs = append(logArgs, "err", err.Error())
				} else {
					logArgs = append(logArgs, "err", "null")
				}

				// Add duration
				logArgs = append(logArgs, "took", fmt.Sprintf("%v", time.Since(begin)))

				logger.Log(logArgs...)
				return response, err
			}
		}
	}

	return Endpoints{
		LoginEndpoint:       opentracing.TraceServer(tracer, "GET /login")(loggingMiddleware("Login")(MakeLoginEndpoint(s))),
		RegisterEndpoint:    opentracing.TraceServer(tracer, "POST /register")(loggingMiddleware("Register")(MakeRegisterEndpoint(s))),
		HealthEndpoint:      MakeHealthEndpoint(s), // No tracing for health checks
		UserGetEndpoint:     opentracing.TraceServer(tracer, "GET /customers")(loggingMiddleware("GetUsers")(MakeUserGetEndpoint(s))),
		UserPostEndpoint:    opentracing.TraceServer(tracer, "POST /customers")(loggingMiddleware("PostUser")(MakeUserPostEndpoint(s))),
		AddressGetEndpoint:  opentracing.TraceServer(tracer, "GET /addresses")(loggingMiddleware("GetAddresses")(MakeAddressGetEndpoint(s))),
		AddressPostEndpoint: opentracing.TraceServer(tracer, "POST /addresses")(loggingMiddleware("PostAddress")(MakeAddressPostEndpoint(s))),
		CardGetEndpoint:     opentracing.TraceServer(tracer, "GET /cards")(loggingMiddleware("GetCards")(MakeCardGetEndpoint(s))),
		DeleteEndpoint:      opentracing.TraceServer(tracer, "DELETE /")(loggingMiddleware("Delete")(MakeDeleteEndpoint(s))),
		CardPostEndpoint:    opentracing.TraceServer(tracer, "POST /cards")(loggingMiddleware("PostCard")(MakeCardPostEndpoint(s))),
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
		db.SetTraceContext(ctx)
		req := request.(loginRequest)
		u, err := s.Login(req.Username, req.Password)
		return userResponse{User: u}, err
	}
}

// MakeRegisterEndpoint returns an endpoint via the given service.
func MakeRegisterEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		db.SetTraceContext(ctx)
		req := request.(registerRequest)
		id, err := s.Register(req.Username, req.Password, req.Email, req.FirstName, req.LastName)
		return postResponse{ID: id}, err
	}
}

// MakeUserGetEndpoint returns an endpoint via the given service.
func MakeUserGetEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		db.SetTraceContext(ctx)
		req := request.(GetRequest)

		usrs, err := s.GetUsers(req.ID)
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
		db.GetUserAttributes(&user)
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
		db.SetTraceContext(ctx)
		req := request.(users.User)
		id, err := s.PostUser(req)
		return postResponse{ID: id}, err
	}
}

// MakeAddressGetEndpoint returns an endpoint via the given service.
func MakeAddressGetEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		db.SetTraceContext(ctx)
		req := request.(GetRequest)
		adds, err := s.GetAddresses(req.ID)
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
		db.SetTraceContext(ctx)
		req := request.(addressPostRequest)
		id, err := s.PostAddress(req.Address, req.UserID)
		return postResponse{ID: id}, err
	}
}

// MakeCardGetEndpoint returns an endpoint via the given service.
func MakeCardGetEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		db.SetTraceContext(ctx)
		req := request.(GetRequest)
		cards, err := s.GetCards(req.ID)
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
		db.SetTraceContext(ctx)
		req := request.(cardPostRequest)
		id, err := s.PostCard(req.Card, req.UserID)
		return postResponse{ID: id}, err
	}
}

// MakeDeleteEndpoint returns an endpoint via the given service.
func MakeDeleteEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		db.SetTraceContext(ctx)
		req := request.(deleteRequest)
		err = s.Delete(req.Entity, req.ID)
		if err == nil {
			return statusResponse{Status: true}, err
		}
		return statusResponse{Status: false}, err
	}
}

// MakeHealthEndpoint returns current health of the given service.
func MakeHealthEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		health := s.Health()
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
