package bigquery

import (
	"errors"

	"golang.org/x/net/context"

	"google.golang.org/appengine/log"

	bq "cloud.google.com/go/bigquery"
)

var (
	// ErrJobWorking is JobのStatusがDONEになってない時のエラー
	ErrJobWorking = errors.New("job working")
)

// Query is Run Query
func Query(c context.Context, projectID string, defaultDatasetID string, query string, option *bq.QueryConfig) (*bq.Job, error) {
	client, err := bq.NewClient(c, projectID)
	if err != nil {
		log.Errorf(c, "bigquery.NewClient err = %v", err)
		return nil, err
	}
	q := client.Query(query)
	if option != nil {
		q.QueryConfig = *option
	}

	q.DefaultProjectID = projectID
	q.DefaultDatasetID = defaultDatasetID

	j, err := q.Run(c)
	if err != nil {
		log.Errorf(c, "bigquery.Query.Run err = %v", err)
		return nil, err
	}
	return j, nil
}

// GetQueryJobResult is 引数で渡したjobIDのQueryの結果を取得する
func GetQueryJobResult(c context.Context, projectID string, jobID string) ([]bq.ValueList, []bq.Schema, error) {
	client, err := bq.NewClient(c, projectID)
	if err != nil {
		log.Errorf(c, "bigquery.NewClient err = %v", err)
		return nil, nil, err
	}

	job, err := client.JobFromID(c, jobID)
	if err != nil {
		log.Errorf(c, "bigquery.JobFromID err = %v", err)
		return nil, nil, err
	}

	js, err := job.Status(c)
	if err != nil {
		log.Errorf(c, "bigquery.Job.Status err = %v", err)
		return nil, nil, err
	}
	if js.Done() == false {
		return nil, nil, ErrJobWorking
	}

	if js.Err() != nil {
		log.Errorf(c, "bigquery.JobStatus err = %v", err)
		return nil, nil, js.Err()
	}

	it, err := job.Read(c)
	if err != nil {
		log.Errorf(c, "bigquery.Job.Read err = %v", err)
		return nil, nil, err
	}

	var sl []bq.Schema
	var vl []bq.ValueList
	for it.Next(c) {
		// Retrieve the current row into a list of values.
		var values bq.ValueList
		if err := it.Get(&values); err != nil {
			log.Errorf(c, "bigquery.Iterator.Get err = %v", err)
			return nil, nil, err
		}
		vl = append(vl, values)

		schema, err := it.Schema()
		if err != nil {
			log.Errorf(c, "bigquery.Schema err = %v", err)
			return nil, nil, err
		}
		sl = append(sl, schema)
	}
	if it.Err() != nil {
		log.Errorf(c, "bigquery.Iterator.Err err = %v", err)
		return nil, nil, err
	}

	return vl, sl, nil
}
