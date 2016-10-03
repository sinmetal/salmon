package example

import (
	"encoding/json"
	"net/http"

	"google.golang.org/appengine"

	salmon "salmon/v0"
)

type computeService struct {
}

func init() {
	cs := computeService{}

	http.HandleFunc("/compute/instances", cs.ListInstance)
}

func (cs *computeService) ListInstance(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	l, err := salmon.ListInstance(c, []string{"cp300demo1"})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	j, err := json.Marshal(l)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}
