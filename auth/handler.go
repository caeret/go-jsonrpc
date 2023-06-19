package auth

import (
	"context"
	"github.com/caeret/logging"
	"net/http"
	"strings"
)

type Handler struct {
	Logger logging.Logger
	Verify func(ctx context.Context, token string) ([]Permission, error)
	Next   http.HandlerFunc
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	token := r.Header.Get("Authorization")
	if token == "" {
		token = r.FormValue("token")
		if token != "" {
			token = "Bearer " + token
		}
	}

	if token != "" {
		if !strings.HasPrefix(token, "Bearer ") {
			h.Logger.Warn("missing Bearer prefix in auth header")
			w.WriteHeader(401)
			return
		}
		token = strings.TrimPrefix(token, "Bearer ")

		allow, err := h.Verify(ctx, token)
		if err != nil {
			h.Logger.Warn("JWT Verification failed", "from", r.RemoteAddr, "error", err)
			w.WriteHeader(401)
			return
		}

		ctx = WithPerm(ctx, allow)
	}

	h.Next(w, r.WithContext(ctx))
}
