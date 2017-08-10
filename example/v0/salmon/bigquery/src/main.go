package main

import (
	"flag"

	"context"
)

func main() {
	var funcVar string
	flag.StringVar(&funcVar, "func", "query", "func=query")

	var projectIDVar string
	flag.StringVar(&projectIDVar, "projectID", "yourID", "projectID=yourID")

	var jobIDVar string
	flag.StringVar(&jobIDVar, "jobID", "yourID", "jobID=yourID")

	flag.Parse()

	c := context.Background()
	switch funcVar {
	case "query":
		query(c, projectIDVar)
	case "get_query_job_result":
		getQueryJobResult(c, projectIDVar, jobIDVar)
	}
}
