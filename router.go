package etp

import (
	"errors"
	"strings"
)

var (
	ErrRouterCompiled     = errors.New("transport: router is already compiled")
	ErrRouterNotCompiled  = errors.New("transport: router is not compiled")
	ErrRouteNotFound      = errors.New("transport: route not found")
	ErrRouteAlreadyExists = errors.New("transport: route already exists")
	ErrRoutePatternEmpty  = errors.New("transport: route pattern is empty")
	ErrGroupPrefixCount   = errors.New("transport: group accepts at most one prefix")
	ErrMiddlewareNil      = errors.New("transport: middleware is nil")
	ErrHandlerNil         = errors.New("transport: handler is nil")
)

type middlewareRoute struct {
	pattern string
	fn      Middleware
}

type endpointRoute struct {
	pattern string
	fn      Handler
}

// Router is the route-registration surface shared by App and Group.
// Controllers should depend on this interface rather than a concrete transport.
type Router interface {
	// On registers handler for one exact event name, for example
	// "control.workspace.get". Event names are combined with the optional
	// Group prefix when the router is compiled. It returns ErrRoutePatternEmpty
	// for an empty name, ErrRouteAlreadyExists for a duplicate route,
	// ErrHandlerNil for a nil handler, or ErrRouterCompiled after Compile.
	On(event string, handler Handler) error

	// Use registers middleware for matching routes. pattern may be an exact
	// event name, a namespace wildcard such as "control.*", or "*" for every
	// route in the current group. Middleware runs in registration order before
	// the matching handler. It returns ErrMiddlewareNil for a nil middleware or
	// ErrRouterCompiled after Compile.
	Use(pattern string, middleware Middleware) error

	// Group creates a nested route scope. It accepts zero arguments for a group
	// without a prefix, or one prefix such as "control". Routes registered on a
	// prefixed group are combined with that prefix: group.On("workspace.get", h)
	// registers "control.workspace.get". Middleware registered on parent groups
	// is inherited by child groups. Group panics with ErrRouterCompiled after
	// Compile or ErrGroupPrefixCount when more than one prefix is supplied.
	Group(prefix ...string) *Group
}

type compiledRouter struct {
	prefix      string
	parent      *compiledRouter
	middlewares []middlewareRoute
	endpoints   []endpointRoute
	groups      []*compiledRouter
	compiled    map[string]Handler
	registered  map[string]struct{}
	isCompiled  bool
}

func NewRouter() *compiledRouter {
	return &compiledRouter{registered: make(map[string]struct{})}
}

func (r *compiledRouter) Use(pattern string, middleware Middleware) error {
	if r.isCompiled {
		return ErrRouterCompiled
	}
	if middleware == nil {
		return ErrMiddlewareNil
	}
	r.middlewares = append(r.middlewares, middlewareRoute{
		pattern: r.scopedMiddlewarePattern(pattern),
		fn:      middleware,
	})
	return nil
}

func (r *compiledRouter) On(pattern string, handler Handler) error {
	if r.isCompiled {
		return ErrRouterCompiled
	}
	if handler == nil {
		return ErrHandlerNil
	}
	fullPattern := joinPattern(r.fullPrefix(), pattern)
	if fullPattern == "" {
		return ErrRoutePatternEmpty
	}
	root := r.root()
	if _, ok := root.registered[fullPattern]; ok {
		return ErrRouteAlreadyExists
	}
	root.registered[fullPattern] = struct{}{}
	r.endpoints = append(r.endpoints, endpointRoute{pattern: fullPattern, fn: handler})
	return nil
}

func (r *compiledRouter) Group(prefix ...string) *compiledRouter {
	if r.isCompiled {
		panic(ErrRouterCompiled)
	}
	if len(prefix) > 1 {
		panic(ErrGroupPrefixCount)
	}
	groupPrefix := ""
	if len(prefix) == 1 {
		groupPrefix = prefix[0]
	}
	group := &compiledRouter{prefix: groupPrefix, parent: r}
	r.groups = append(r.groups, group)
	return group
}

func (r *compiledRouter) Compile() {
	root := r.root()
	if root.isCompiled {
		return
	}
	compiled := make(map[string]Handler, len(root.registered))
	var middlewares []middlewareRoute
	root.compileInto(compiled, middlewares)
	root.compiled = compiled
	root.isCompiled = true
}

func (r *compiledRouter) Emit(ctx *Context) error {
	root := r.root()
	if !root.isCompiled {
		return ErrRouterNotCompiled
	}
	handler, ok := root.compiled[ctx.Event]
	if !ok {
		return ErrRouteNotFound
	}
	return handler(ctx)
}

func (r *compiledRouter) compileInto(compiled map[string]Handler, inherited []middlewareRoute) {
	middlewares := append(inherited, r.middlewares...)
	for _, endpoint := range r.endpoints {
		handler := endpoint.fn
		for i := len(middlewares) - 1; i >= 0; i-- {
			middleware := middlewares[i]
			if matchPattern(middleware.pattern, endpoint.pattern) {
				handler = middleware.fn(handler)
			}
		}
		compiled[endpoint.pattern] = handler
	}
	for _, group := range r.groups {
		group.compileInto(compiled, middlewares)
	}
}

func (r *compiledRouter) scopedMiddlewarePattern(pattern string) string {
	prefix := r.fullPrefix()
	if pattern == "" || pattern == "*" {
		if prefix == "" {
			return "*"
		}
		return prefix + ".*"
	}
	return joinPattern(prefix, pattern)
}

func (r *compiledRouter) fullPrefix() string {
	if r == nil {
		return ""
	}
	var parts []string
	for current := r; current != nil; current = current.parent {
		if current.prefix != "" {
			parts = append(parts, current.prefix)
		}
	}
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return strings.Join(parts, ".")
}

func (r *compiledRouter) root() *compiledRouter {
	for r.parent != nil {
		r = r.parent
	}
	return r
}

func joinPattern(prefix, pattern string) string {
	prefix = strings.Trim(prefix, ".")
	pattern = strings.Trim(pattern, ".")
	if prefix == "" {
		return pattern
	}
	if pattern == "" {
		return prefix
	}
	return prefix + "." + pattern
}

func matchPattern(pattern, event string) bool {
	if pattern == "*" {
		return true
	}
	if before, ok := strings.CutSuffix(pattern, ".*"); ok {
		return strings.HasPrefix(event, before+".")
	}
	return pattern == event
}
