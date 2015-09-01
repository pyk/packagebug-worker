package main

import (
	"database/sql"
	"log"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

var dbconn *sql.DB

func init() {
	var err error
	dbconn, err = sql.Open("postgres", os.Getenv("PACKAGEBUG_DB_TEST"))
	if err != nil {
		log.Fatal(err)
	}
}

var pkgTest = Package{
	Host:  "github.com",
	Owner: "pyk",
	Repo:  "byten",
}

func TestRateUrl(t *testing.T) {
	expected := "root/rate_limit?client_id=id&client_secret=secret"
	urls := pkgTest.RateUrl("root", "id", "secret")
	if urls != expected {
		t.Fatalf("expected: %s got: %s\n", expected, urls)
	}
}

func TestBugUrl(t *testing.T) {
	expected := "root/repos/pyk/byten/issues?client_id=id&client_secret=secret&labels=bug&state=all"
	urls := pkgTest.BugUrl("root", "id", "secret")
	if urls != expected {
		t.Fatalf("expected: %s got: %s\n", expected, urls)
	}
}

func TestPackagePath(t *testing.T) {
	expected := "github.com/pyk/byten"
	path := pkgTest.Path()
	if expected != path {
		t.Errorf("got: %s\n", path)
	}
}

var insertTestDataSQL = `
INSERT INTO packages(package_path,
	package_host, package_owner,
	package_repo, package_etag)
VALUES('test_host/test_owner/test_repo',
	'test_host', 'test_owner', 'test_repo',
	'test_etag');`

var deleteTestDataSQL = `
DELETE FROM packages
WHERE package_host='test_host' AND package_owner='test_owner';`

func TestGetEtag(t *testing.T) {
	// create test data
	_, err := dbconn.Exec(insertTestDataSQL)
	if err != nil {
		t.Fatal(err)
	}
	p := Package{
		Host:  "test_host",
		Owner: "test_owner",
		Repo:  "test_repo",
	}
	etag, err := p.GetEtag(dbconn)
	if err != nil {
		t.Error(err)
	}
	if etag != "test_etag" {
		t.Errorf("got: %s\n", etag)
	}

	// delete test data
	_, err = dbconn.Exec(deleteTestDataSQL)
	if err != nil {
		t.Fatal(err)
	}
}
