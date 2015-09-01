package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/sqs"
	_ "github.com/lib/pq"
)

var (
	PACKAGEBUG_DB                   = os.Getenv("DATABASE_URL")
	PACKAGEBUG_SQS_ENDPOINT         = os.Getenv("PACKAGEBUG_SQS_ENDPOINT")
	PACKAGEBUG_SQS_REGION           = os.Getenv("PACKAGEBUG_SQS_REGION")
	PACKAGEBUG_GITHUB_ROOT_ENDPOINT = os.Getenv("PACKAGEBUG_GITHUB_ROOT_ENDPOINT")
	PACKAGEBUG_GITHUB_CLIENT_ID     = os.Getenv("PACKAGEBUG_GITHUB_CLIENT_ID")
	PACKAGEBUG_GITHUB_CLIENT_SECRET = os.Getenv("PACKAGEBUG_GITHUB_CLIENT_SECRET")
)

// Package represents a Go package
type Package struct {
	Id    string
	Host  string
	Owner string
	Repo  string
}

// Issue represents the issue of package
type Issue struct {
	ApiUrl         string `json:"url"`
	ApiLabelsUrl   string `json:"labels_url"`
	ApiCommentsUrl string `json:"comments_url"`
	ApiEventsUrl   string `json:"events_url"`
	Url            string `json:"html_url"`
	GithubId       string `json:"id"`
	Id             string
	Number         int    `json:"number"`
	Title          string `json:"title"`
}

type IssueCreator struct {
	Username        string `json:"login"`
	GithubId        string `json:"id"`
	AvatarUrl       string `json:"avatar_url"`
	GravatarId      string `json:"gravatar_id"`
	ApiProfileUrl   string `json:"url"`
	ProfileUrl      string `json:"html_url"`
	ApiFollowersUrl string `json:"followers_url"`
	ApiFollowingUrl string `json:"following_url"`
	ApiGistsUrl     string `json:"gists_url"`
	ApiStarredUrl   string `json:"starred_url"`
}

// Path returns valid import path of the package
func (p Package) Path() string {
	return fmt.Sprintf("%s/%s/%s", p.Host, p.Owner, p.Repo)
}

// GetEtag get etag data of the last fetch operation on the database. It returns
// complete etag if exists, otherwise empty string.
func (p Package) GetEtag(dbconn *sql.DB) (string, error) {
	var etag sql.NullString

	query := `
	SELECT package_etag
	FROM packages
	WHERE package_path=$1`
	err := dbconn.QueryRow(query, p.Path()).Scan(&etag)
	if err != nil {
		return "", err
	}

	if etag.Valid {
		return etag.String, nil
	}

	return "", nil
}

// BugUrl returns the url where the bugs is fetched from.
func (p Package) BugUrl(root, id, secret string) string {
	if p.Host == "github.com" {
		query := url.Values{}
		query.Add("client_id", id)
		query.Add("client_secret", secret)
		query.Add("state", "all")
		query.Add("labels", "bug")
		return fmt.Sprintf("%s/repos/%s/%s/issues?%s", root,
			p.Owner, p.Repo, query.Encode())
	}
	return ""
}

// FetchBug fetch bugs from package repository via the corresponding API.
func (p Package) FetchBug(wg *sync.WaitGroup, dbconn *sql.DB) {
	// for package hosted on github
	if p.Host == "github.com" {
		// get etag data of last fetch operation from the database
		etag, err := p.GetEtag(dbconn)
		if err != nil {
			log.Printf("[worker] failed to get etag: %s\n", err)
			wg.Done()
			return
		}

		urls := p.BugUrl(PACKAGEBUG_GITHUB_ROOT_ENDPOINT,
			PACKAGEBUG_GITHUB_CLIENT_ID, PACKAGEBUG_GITHUB_CLIENT_SECRET)
		// setup http client and request
		client := &http.Client{}
		req, err := http.NewRequest("GET", urls, nil)
		if err != nil {
			log.Printf("[worker] error create request: %s\n", err)
			wg.Done()
			return
		}

		// setup request header
		req.Header.Add("User-Agent", "pyk")
		req.Header.Add("Accept", "application/vnd.github.v3+json")
		// use conditional request if possible
		if etag != "" {
			req.Header.Add("If-None-Match", etag)
		}

		// do the request
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("[worker] error fetch: %s\n", err)
			wg.Done()
			return
		}
		defer resp.Body.Close()
		log.Printf("[worker] fetch %s %s %s\n", p.Path(), resp.Status, urls)
		if resp.StatusCode == 200 {
			// package exists

		}
	}
	// insert bugs to the database
	// process successful
	wg.Done()
}

// RateURL returns the URL where to check the current status of rate limit.
func (p Package) RateUrl(root, id, secret string) string {
	if p.Host == "github.com" {
		query := url.Values{}
		query.Add("client_id", id)
		query.Add("client_secret", secret)
		return fmt.Sprintf("%s/rate_limit?%s", root, query.Encode())
	}
	return ""
}

// CheckRateLimit check rate limit of API request for a package. If error happen
// the rate limit will be -1.
func (p Package) CheckRateLimit() (int, int64, error) {
	// for package hosted on github
	if p.Host == "github.com" {
		urls := p.RateUrl(PACKAGEBUG_GITHUB_ROOT_ENDPOINT,
			PACKAGEBUG_GITHUB_CLIENT_ID, PACKAGEBUG_GITHUB_CLIENT_SECRET)
		// send request
		resp, err := http.Get(urls)
		if err != nil {
			return -1, -1, err
		}
		defer resp.Body.Close()

		// get remaining limit
		limit := resp.Header.Get("X-RateLimit-Remaining")
		rateLimit, err := strconv.Atoi(limit)
		if err != nil {
			return -1, -1, err
		}

		// get time reset
		reset := resp.Header.Get("X-RateLimit-Reset")
		resetTime, err := strconv.ParseInt(reset, 10, 64)
		if err != nil {
			return -1, -1, err
		}

		return rateLimit, resetTime, nil
	}
	return -1, -1, errors.New("host not supported")
}

func main() {
	// connect to the database
	dbconn, err := sql.Open("postgres", PACKAGEBUG_DB)
	if err != nil {
		log.Fatal(err)
	}

	// make sure the database up
	err = dbconn.Ping()
	if err != nil {
		log.Fatal(err)
	}

	// set up aws SDK credentials & config
	cred := credentials.NewEnvCredentials()
	_, err = cred.Get()
	if err != nil {
		log.Fatal(err)
	}
	config := aws.NewConfig()
	config.Credentials = cred
	config.Endpoint = aws.String(PACKAGEBUG_SQS_ENDPOINT)
	config.Region = aws.String(PACKAGEBUG_SQS_REGION)

	sqsconn := sqs.New(config)
	log.Println("[worker] service started ...")

	// setup ReceiveMessageInput parameter
	params := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(1),
		QueueUrl:            aws.String(PACKAGEBUG_SQS_ENDPOINT),
		WaitTimeSeconds:     aws.Int64(10),
	}

	wg := new(sync.WaitGroup)
	nworker := 1
	for {
		// wait 10s until message received
		resp, err := sqsconn.ReceiveMessage(params)
		if err != nil {
			log.Printf("[worker] receive message: %s\n", err)
			continue
		}

		// only process if message exists, otherwise retry the request.
		if resp.Messages != nil {
			// get package info from message body
			var p Package
			msg := strings.Split(*resp.Messages[0].Body, ",")
			if len(msg) != 4 {
				log.Println("[worker] invalid message body.")
				continue
			}
			p.Id = msg[0]
			p.Host = msg[1]
			p.Owner = msg[2]
			p.Repo = msg[3]

			// check rate limit of API request before do the heavy task
			// if limit exceed then pause the worker until the limit is reset.
			rate, reset, err := p.CheckRateLimit()
			if err != nil {
				log.Printf("[worker] check rate limit: %s\n", err)
				continue
			}

			if rate > 0 {
				// for performance reason, there are only 10 worker process running
				// at the same time.
				if nworker <= 10 {
					wg.Add(1)
					go p.FetchBug(wg, dbconn)
					nworker++
				} else {
					nworker = 0
					log.Println("[worker] wait 10 worker process finished")
					wg.Wait()
				}
			} else {
				// rate limit exceed wait until rate limit reset
				now := time.Now().Unix()
				wait := reset - now
				log.Printf("[worker] rate limit exceed. wait %ds to reset.\n", wait)
				<-time.After(time.Duration(wait) * time.Second)
				log.Println("[worker] rate limit reset")
				continue
			}

		} else {
			log.Println("[worker] empty message received. retry request.")
			continue
		}
	}
}
