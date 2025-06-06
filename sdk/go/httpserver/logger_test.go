// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"git.arvados.org/arvados.git/sdk/go/ctxlog"
	"github.com/sirupsen/logrus"
	check "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&Suite{})

type Suite struct {
	ctx     context.Context
	log     *logrus.Logger
	logdata *bytes.Buffer
}

func (s *Suite) SetUpTest(c *check.C) {
	s.logdata = bytes.NewBuffer(nil)
	s.log = logrus.New()
	s.log.Out = s.logdata
	s.log.Formatter = &logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	}
	s.ctx = ctxlog.Context(context.Background(), s.log)
}

func (s *Suite) TestWithDeadline(c *check.C) {
	req, err := http.NewRequest("GET", "https://foo.example/bar", nil)
	c.Assert(err, check.IsNil)

	// Short timeout cancels context in <1s
	resp := httptest.NewRecorder()
	HandlerWithDeadline(time.Millisecond, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		select {
		case <-req.Context().Done():
			w.Write([]byte("ok"))
		case <-time.After(time.Second):
			c.Error("timed out")
		}
	})).ServeHTTP(resp, req.WithContext(s.ctx))
	c.Check(resp.Body.String(), check.Equals, "ok")

	// Long timeout does not cancel context in <1ms
	resp = httptest.NewRecorder()
	HandlerWithDeadline(time.Second, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		select {
		case <-req.Context().Done():
			c.Error("request context done too soon")
		case <-time.After(time.Millisecond):
			w.Write([]byte("ok"))
		}
	})).ServeHTTP(resp, req.WithContext(s.ctx))
	c.Check(resp.Body.String(), check.Equals, "ok")
}

func (s *Suite) TestExemptFromDeadline(c *check.C) {
	srv := Server{
		Addr: ":",
		Server: http.Server{
			Handler: HandlerWithDeadline(time.Millisecond, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/exempt" {
					ExemptFromDeadline(req)
				}
				time.Sleep(time.Second / 10)
				if req.Context().Err() != nil {
					w.WriteHeader(499)
					return
				}
				fmt.Fprint(w, "ok")
			})),
			BaseContext: func(net.Listener) context.Context { return s.ctx },
		},
	}
	srv.Start()
	defer srv.Close()

	resp, err := http.Get("http://" + srv.Addr + "/normal")
	c.Assert(err, check.IsNil)
	c.Check(resp.StatusCode, check.Equals, 499)
	body, err := ioutil.ReadAll(resp.Body)
	c.Check(err, check.IsNil)
	c.Check(string(body), check.Equals, "")

	resp, err = http.Get("http://" + srv.Addr + "/exempt")
	c.Assert(err, check.IsNil)
	c.Check(resp.StatusCode, check.Equals, 200)
	body, err = ioutil.ReadAll(resp.Body)
	c.Check(err, check.IsNil)
	c.Check(string(body), check.Equals, "ok")
}

func (s *Suite) TestLogRequests(c *check.C) {
	h := AddRequestIDs(LogRequests(
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Write([]byte("hello world"))
		})))

	req, err := http.NewRequest("GET", "https://foo.example/bar", nil)
	req.Header.Set("X-Forwarded-For", "1.2.3.4:12345")
	c.Assert(err, check.IsNil)
	resp := httptest.NewRecorder()

	h.ServeHTTP(resp, req.WithContext(s.ctx))

	dec := json.NewDecoder(s.logdata)

	gotReq := make(map[string]interface{})
	err = dec.Decode(&gotReq)
	c.Check(err, check.IsNil)
	c.Logf("%#v", gotReq)
	c.Check(gotReq["RequestID"], check.Matches, "req-[a-z0-9]{20}")
	c.Check(gotReq["reqForwardedFor"], check.Equals, "1.2.3.4:12345")
	c.Check(gotReq["msg"], check.Equals, "request")

	gotResp := make(map[string]interface{})
	err = dec.Decode(&gotResp)
	c.Check(err, check.IsNil)
	c.Logf("%#v", gotResp)
	c.Check(gotResp["RequestID"], check.Equals, gotReq["RequestID"])
	c.Check(gotResp["reqForwardedFor"], check.Equals, "1.2.3.4:12345")
	c.Check(gotResp["msg"], check.Equals, "response")

	c.Assert(gotResp["time"], check.FitsTypeOf, "")
	_, err = time.Parse(time.RFC3339Nano, gotResp["time"].(string))
	c.Check(err, check.IsNil)

	for _, key := range []string{"timeToStatus", "timeWriteBody", "timeTotal"} {
		c.Assert(gotResp[key], check.FitsTypeOf, float64(0))
		c.Check(gotResp[key].(float64), check.Not(check.Equals), float64(0))
	}
}

func (s *Suite) TestLogErrorBody(c *check.C) {
	dec := json.NewDecoder(s.logdata)

	for _, trial := range []struct {
		label      string
		statusCode int
		sentBody   string
		expectLog  bool
		expectBody string
	}{
		{"ok", 200, "hello world", false, ""},
		{"redir", 302, "<a href='http://foo.example/baz'>redir</a>", false, ""},
		{"4xx short body", 400, "oops", true, "oops"},
		{"4xx long body", 400, fmt.Sprintf("%0*d", sniffBytes*2, 1), true, fmt.Sprintf("%0*d", sniffBytes, 0)},
		{"5xx empty body", 500, "", true, ""},
	} {
		comment := check.Commentf("in trial: %q", trial.label)

		req, err := http.NewRequest("GET", "https://foo.example/bar", nil)
		c.Assert(err, check.IsNil)
		resp := httptest.NewRecorder()

		LogRequests(
			http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(trial.statusCode)
				w.Write([]byte(trial.sentBody))
			}),
		).ServeHTTP(resp, req.WithContext(s.ctx))

		gotReq := make(map[string]interface{})
		err = dec.Decode(&gotReq)
		c.Check(err, check.IsNil)
		c.Logf("%#v", gotReq)
		gotResp := make(map[string]interface{})
		err = dec.Decode(&gotResp)
		c.Check(err, check.IsNil)
		c.Logf("%#v", gotResp)
		if trial.expectLog {
			c.Check(gotResp["respBody"], check.Equals, trial.expectBody, comment)
		} else {
			c.Check(gotResp["respBody"], check.IsNil, comment)
		}
	}
}
