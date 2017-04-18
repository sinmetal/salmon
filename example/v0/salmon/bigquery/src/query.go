package main

import (
	"encoding/json"
	"fmt"

	"golang.org/x/net/context"

	sbq "github.com/sinmetal/salmon/v0/salmon/bigquery"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

func query(c context.Context, projectID string) {
	job, err := sbq.Query(c, projectID, &bigquery.QueryConfig{
		AllowLargeResults: false,
		DefaultDatasetID:  "work",
		DefaultProjectID:  projectID,
		UseStandardSQL:    false,
		Q:                 "SELECT \"A\" As Name, 1 As Number",
	})
	if err != nil {
		fmt.Printf("bigquery.Query err.\n%s\n", err.Error())
		return
	}
	fmt.Printf("bigquery.JobID = %s", job.ID())
}

// QueryResultRow is サンプルクエリの結果の1行と同じスキーマのstruct
type QueryResultRow struct {
	Name   string
	Number int64
}

// Load is bigquery.ValuerLoader interface
func (r *QueryResultRow) Load(v []bigquery.Value, s bigquery.Schema) error {
	for i, val := range v {
		switch s[i].Name {
		case "Name":
			r.Name = val.(string)
		case "Number":
			r.Number = val.(int64)
		default:
			return fmt.Errorf("unexpected Index. i = %d, v = %v", i, val)
		}
	}
	return nil
}

func getQueryJobResult(c context.Context, projectID, jobID string) {
	fmt.Printf("ProjectID = %s, JobID = %s\n", projectID, jobID)

	it, err := sbq.GetQueryJobResult(c, projectID, jobID)
	if err != nil {
		fmt.Printf("bigquery.GetQueryJobResult err.\n%s\n", err)
		return
	}
	var rd []QueryResultRow
	for {
		var row QueryResultRow
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Printf("bigquery.GetQueryJobResult err.\n%s\n", err.Error())
			return
		}
		rd = append(rd, row)
	}
	j, err := json.Marshal(rd)
	if err != nil {
		fmt.Printf("json.Marshal err.\n%s\n", err.Error())
		return
	}
	fmt.Printf("%s\n", j)
}
