package httpio

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"sync"

	"github.com/caeret/logging"
	"github.com/google/uuid"
	"golang.org/x/xerrors"

	jsonrpc "github.com/caeret/go-jsonrpc"
)

func ReaderParamEncoder(addr string) jsonrpc.Option {
	return jsonrpc.WithParamEncoder(new(io.Reader), func(value reflect.Value) (reflect.Value, error) {
		r := value.Interface().(io.Reader)

		reqID := uuid.New()
		u, _ := url.Parse(addr)
		u.Path = path.Join(u.Path, reqID.String())

		go func() {
			// TODO: figure out errors here

			resp, err := http.Post(u.String(), "application/octet-stream", r)
			if err != nil {
				logging.Error("sending reader param", "error", err)
				return
			}

			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				logging.Error("sending reader param: non-200 status", "status", resp.Status)
				return
			}
		}()

		return reflect.ValueOf(reqID), nil
	})
}

type waitReadCloser struct {
	io.ReadCloser
	wait chan struct{}
}

func (w *waitReadCloser) Read(p []byte) (int, error) {
	n, err := w.ReadCloser.Read(p)
	if err != nil {
		close(w.wait)
	}
	return n, err
}

func (w *waitReadCloser) Close() error {
	close(w.wait)
	return w.ReadCloser.Close()
}

func ReaderParamDecoder(logger logging.Logger) (http.HandlerFunc, jsonrpc.ServerOption) {
	var readersLk sync.Mutex
	readers := map[uuid.UUID]chan *waitReadCloser{}

	hnd := func(resp http.ResponseWriter, req *http.Request) {
		strId := path.Base(req.URL.Path)
		u, err := uuid.Parse(strId)
		if err != nil {
			http.Error(resp, fmt.Sprintf("parsing reader uuid: %s", err), 400)
		}

		readersLk.Lock()
		ch, found := readers[u]
		if !found {
			ch = make(chan *waitReadCloser)
			readers[u] = ch
		}
		readersLk.Unlock()

		wr := &waitReadCloser{
			ReadCloser: req.Body,
			wait:       make(chan struct{}),
		}

		select {
		case ch <- wr:
		case <-req.Context().Done():
			logger.Error("context error in reader stream handler (1)", "error", req.Context().Err())
			resp.WriteHeader(500)
			return
		}

		select {
		case <-wr.wait:
		case <-req.Context().Done():
			logger.Error("context error in reader stream handler (2)", "error", req.Context().Err())
			resp.WriteHeader(500)
			return
		}

		resp.WriteHeader(200)
	}

	dec := jsonrpc.WithParamDecoder(new(io.Reader), func(ctx context.Context, b []byte) (reflect.Value, error) {
		var strId string
		if err := json.Unmarshal(b, &strId); err != nil {
			return reflect.Value{}, xerrors.Errorf("unmarshaling reader id: %w", err)
		}

		u, err := uuid.Parse(strId)
		if err != nil {
			return reflect.Value{}, xerrors.Errorf("parsing reader UUDD: %w", err)
		}

		readersLk.Lock()
		ch, found := readers[u]
		if !found {
			ch = make(chan *waitReadCloser)
			readers[u] = ch
		}
		readersLk.Unlock()

		select {
		case wr := <-ch:
			return reflect.ValueOf(wr), nil
		case <-ctx.Done():
			return reflect.Value{}, ctx.Err()
		}
	})

	return hnd, dec
}
