package gazette

import (
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/trace"

	"github.com/LiveRamp/gazette/journal"
	"github.com/LiveRamp/gazette/metrics"
)

type ReplicateAPI struct {
	handler ReplicateOpHandler
	decoder *schema.Decoder
}

func NewReplicateAPI(handler ReplicateOpHandler) *ReplicateAPI {
	decoder := schema.NewDecoder()
	decoder.IgnoreUnknownKeys(false)
	decoder.SetAliasTag("json")

	return &ReplicateAPI{handler: handler, decoder: decoder}
}

func (h *ReplicateAPI) Register(router *mux.Router) {
	router.NewRoute().Methods("REPLICATE").HandlerFunc(h.Replicate)
}

func (h *ReplicateAPI) Replicate(w http.ResponseWriter, r *http.Request) {
	r = maybeTrace(r, "ReplicateAPI.Replicate")
	defer finishTrace(r)

	var schema struct {
		WriteHead  int64
		RouteToken string
		NewSpool   bool
	}
	var err error

	if err = r.ParseForm(); err == nil {
		err = h.decoder.Decode(&schema, r.Form)
	}
	if err != nil {
		if tr, ok := trace.FromContext(r.Context()); ok {
			tr.LazyPrintf("parsing request: %v", err)
			tr.SetError()
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var op = journal.ReplicateOp{
		ReplicateArgs: journal.ReplicateArgs{
			Journal:    journal.Name(r.URL.Path[1:]),
			RouteToken: journal.RouteToken(schema.RouteToken),
			WriteHead:  schema.WriteHead,
			NewSpool:   schema.NewSpool,
			Context:    r.Context(),
		},
		Result: make(chan journal.ReplicateResult, 1),
	}

	h.handler.Replicate(op)
	var result = <-op.Result

	if result.Error != nil {
		if result.ErrorWriteHead != 0 {
			w.Header().Add(WriteHeadHeader, strconv.FormatInt(result.ErrorWriteHead, 16))
		}
		http.Error(w, result.Error.Error(), journal.StatusCodeForError(result.Error))
		return
	}
	var n, commitDelta int64

	if n, err = io.Copy(result.Writer, r.Body); err != nil {
		result.Writer.Commit(0) // Abort.
	} else if commitDelta, err = strconv.ParseInt(
		r.Trailer.Get(CommitDeltaHeader), 16, 64); err != nil {
		result.Writer.Commit(0) // Abort.
	} else {
		err = result.Writer.Commit(commitDelta)
	}

	if tr, ok := trace.FromContext(r.Context()); ok {
		tr.LazyPrintf("copied %d bytes / commit %d", n, commitDelta)

		if err != nil {
			tr.LazyPrintf("commit error: %v", err)
			tr.SetError()
		}
	}

	if err != nil {
		log.WithField("err", err).Warn("failed to commit transaction")
		metrics.FailedCommitsTotal.Inc()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent) // Success.
}
