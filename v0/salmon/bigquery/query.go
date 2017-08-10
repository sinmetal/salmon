package bigquery

import (
	"errors"

	"context"

	"google.golang.org/appengine/log"

	bq "cloud.google.com/go/bigquery"
)

var (
	// ErrJobWorking is JobのStatusがDONEになってない時のエラー
	ErrJobWorking = errors.New("job working")

	// ErrValidation is ParameterのValidationに失敗した時のエラー
	ErrValidation = errors.New("validation err")
)

// Query is Run Query
func Query(c context.Context, projectID string, config *bq.QueryConfig) (*bq.Job, error) {
	client, err := bq.NewClient(c, projectID)
	if err != nil {
		log.Errorf(c, "bigquery.NewClient err = %v", err)
		return nil, err
	}
	q := client.Query(config.Q)
	if config == nil {
		log.Errorf(c, "bigquery.QueryConfig required")
		return nil, ErrValidation
	}
	q.QueryConfig = *config

	j, err := q.Run(c)
	if err != nil {
		log.Errorf(c, "bigquery.Query.Run err = %v", err)
		return nil, err
	}
	return j, nil
}

// GetQueryJobResult is 引数で渡したjobIDのQueryの結果を取得する
func GetQueryJobResult(c context.Context, projectID string, jobID string) (*bq.RowIterator, error) {
	client, err := bq.NewClient(c, projectID)
	if err != nil {
		log.Errorf(c, "bigquery.NewClient err = %v", err)
		return nil, err
	}

	job, err := client.JobFromID(c, jobID)
	if err != nil {
		log.Errorf(c, "bigquery.JobFromID err = %v", err)
		return nil, err
	}

	js, err := job.Status(c)
	if err != nil {
		log.Errorf(c, "bigquery.Job.Status err = %v", err)
		return nil, err
	}
	if js.Done() == false {
		return nil, ErrJobWorking
	}

	if js.Err() != nil {
		log.Errorf(c, "bigquery.JobStatus err = %v", err)
		return nil, js.Err()
	}

	it, err := job.Read(c)
	if err != nil {
		log.Errorf(c, "bigquery.Job.Read err = %v", err)
		return nil, err
	}

	return it, err
}
