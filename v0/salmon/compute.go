package salmon

import (
	"net/http"
	"sort"
	"sync"

	"context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"google.golang.org/appengine/log"
	"google.golang.org/appengine/urlfetch"

	"google.golang.org/api/compute/v1"
)

// Instances is Compute Engine Instance一覧をProjectID Zoneごとに持つstruct
type Instances struct {
	ProjectID string              `json:"projectID"`
	Zone      string              `json:"zone"`
	Instances []*compute.Instance `json:"instances"`
	Cursor    string              `json:"cursor"`
	Err       error               `json:"err"`
}

// InstancesUnion is ProjectID, ZoneごとのInstances List
type InstancesUnion []*Instances

func (u InstancesUnion) Len() int {
	return len(u)
}

func (u InstancesUnion) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

func (u InstancesUnion) Less(i, j int) bool {
	if u[i].ProjectID < u[j].ProjectID {
		return true
	}
	if u[i].ProjectID == u[j].ProjectID {
		return u[i].Zone < u[j].Zone
	}
	return false
}

// ListZone is Compute Engine Zone List
func ListZone(c context.Context) []string {
	return []string{
		"asia-east1-a",
		"asia-east1-b",
		"asia-east1-c",
		"europe-west1-b",
		"europe-west1-c",
		"europe-west1-d",
		"us-central1-a",
		"us-central1-b",
		"us-central1-c",
		"us-central1-f",
		"us-east1-b",
		"us-east1-c",
		"us-east1-d",
		"us-west1-a",
		"us-west1-b",
	}
}

// ListInstance is Compute Engine Instance 一覧を取得する
// TODO cursor
func ListInstance(c context.Context, projectIDs []string) ([]*Instances, error) {
	s, err := createComputeService(c)
	if err != nil {
		return nil, err
	}

	is := compute.NewInstancesService(s)

	zones := ListZone(c)

	var wg sync.WaitGroup
	receiver := make(chan *Instances)
	go func() {
		for _, projectID := range projectIDs {
			for _, zone := range zones {
				wg.Add(1)
				go func(projectID string, zone string, cursor string) {
					instances, cursor, err := listInstance(c, is, projectID, zone)
					if err != nil {
						receiver <- &Instances{
							Err: err,
						}
					} else {
						receiver <- &Instances{
							ProjectID: projectID,
							Zone:      zone,
							Instances: instances,
							Cursor:    cursor,
							Err:       nil,
						}
					}
					wg.Done()
				}(projectID, zone, "")
			}
		}
		wg.Wait()
		close(receiver)
	}()

	var results InstancesUnion
	for {
		receive, ok := <-receiver
		if !ok {
			break
		}
		if receive.Instances == nil && receive.Err == nil {
			continue
		}
		results = append(results, receive)
	}
	sort.Sort(results)
	return results, nil
}

func createComputeService(c context.Context) (*compute.Service, error) {
	source, err := google.DefaultTokenSource(c, compute.ComputeScope)
	if err != nil {
		return nil, err
	}
	client := &http.Client{
		Transport: &oauth2.Transport{
			Source: source,
			Base:   &urlfetch.Transport{Context: c},
		},
	}
	s, err := compute.New(client)
	if err != nil {
		log.Errorf(c, "compute.New: %s", err)
		return nil, err
	}
	return s, nil
}

// listInstance is Compute Engine List
func listInstance(c context.Context, is *compute.InstancesService, projectName string, zone string) ([]*compute.Instance, string, error) {
	ilc := is.List(projectName, zone)
	il, err := ilc.Do()
	if err != nil {
		return nil, "", err
	}
	return il.Items, il.NextPageToken, nil
}
