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

type Router struct {
	prefix      string
	parent      *Router
	middlewares []middlewareRoute
	endpoints   []endpointRoute
	groups      []*Router
	compiled    map[string]Handler
	registered  map[string]struct{}
	isCompiled  bool
}

func NewRouter() *Router {
	return &Router{registered: make(map[string]struct{})}
}

func (r *Router) Use(pattern string, middleware Middleware) error {
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

func (r *Router) On(pattern string, handler Handler) error {
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

func (r *Router) Group(prefix string, middlewares ...Middleware) *Router {
	if r.isCompiled {
		panic(ErrRouterCompiled)
	}
	group := &Router{prefix: prefix, parent: r}
	for _, middleware := range middlewares {
		if middleware == nil {
			panic(ErrMiddlewareNil)
		}
		group.middlewares = append(group.middlewares, middlewareRoute{
			pattern: group.scopedMiddlewarePattern("*"),
			fn:      middleware,
		})
	}
	r.groups = append(r.groups, group)
	return group
}

func (r *Router) Compile() {
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

func (r *Router) Emit(ctx *Context) error {
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

func (r *Router) compileInto(compiled map[string]Handler, inherited []middlewareRoute) {
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

func (r *Router) scopedMiddlewarePattern(pattern string) string {
	prefix := r.fullPrefix()
	if pattern == "" || pattern == "*" {
		if prefix == "" {
			return "*"
		}
		return prefix + ".*"
	}
	return joinPattern(prefix, pattern)
}

func (r *Router) fullPrefix() string {
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

func (r *Router) root() *Router {
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
