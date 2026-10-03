/*
Copyright (c) 2019, Percona LLC.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

// Package slowparse parses a MySQL or MariaDB slow query log.
//
// It is a fork of github.com/percona/go-mysql/log/slow (BSD-3-Clause, see
// LICENSE) with two changes, marked "cnmsql:" below:
//
//   - The statement text is accumulated in a builder capped at
//     Options.MaxQueryBytes. Upstream concatenates strings line by line, which
//     is quadratic: a 200k-line statement took 18 seconds to parse.
//   - Only "# Time:" and "# User@Host:" end a statement. Upstream ends it at
//     any "# <Capital>" line, which cut statements at a SQL comment line.
//
// Debug logging, admin command filtering and the event fields cnmsql does not
// use are left out.
package slowparse

import (
	"bufio"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Event is one slow log entry.
type Event struct {
	Offset         uint64    // byte offset in file at which event starts
	OffsetEnd      uint64    // byte offset in file at which event ends
	Ts             time.Time // timestamp of event
	Admin          bool      // true if Query is admin command
	Query          string    // SQL query or admin command
	QueryTruncated bool      // Query was cut at Options.MaxQueryBytes
	User           string
	Host           string
	Db             string
	TimeMetrics    map[string]float64 // *_time and *_wait metrics
	NumberMetrics  map[string]uint64  // most metrics
	BoolMetrics    map[string]bool    // yes/no metrics
	RateType       string             // Percona Server rate limit type
	RateLimit      uint               // Percona Server rate limit value
}

// NewEvent returns an Event with its metric maps allocated.
func NewEvent() *Event {
	return &Event{
		TimeMetrics:   make(map[string]float64),
		NumberMetrics: make(map[string]uint64),
		BoolMetrics:   make(map[string]bool),
	}
}

// Options configure a Parser.
type Options struct {
	StartOffset uint64 // byte offset in file at which to start parsing
	// DefaultLocation is the location of timestamps in the old
	// "# Time: YYMMDD HH:MM:SS" format, which MariaDB still writes.
	DefaultLocation *time.Location
	// MaxQueryBytes caps Event.Query; 0 means no cap.
	MaxQueryBytes int
}

var (
	timeRe    = regexp.MustCompile(`Time: (\S+\s{1,2}\S+)`)
	timeNewRe = regexp.MustCompile(`Time:\s+(\d{4}-\d{2}-\d{2}\S+)`)
	userRe    = regexp.MustCompile(`User@Host: ([^\[]+|\[[^[]+\]).*?@ (\S*) \[(.*)\]`)
	schema    = regexp.MustCompile(`Schema: +(.*?) +Last_errno:`)
	headerRe  = regexp.MustCompile(`^#\s+[A-Z]`)
	metricsRe = regexp.MustCompile(`(\w+): (\S+|\z)`)
	adminRe   = regexp.MustCompile(`command: (.+)`)
	setRe     = regexp.MustCompile(`^SET (?:last_insert_id|insert_id|timestamp)`)
	useRe     = regexp.MustCompile(`^(?i)use `)
)

// Parser parses a slow log file from an offset to its end.
type Parser struct {
	file *os.File
	opt  Options

	stopChan    chan bool
	eventChan   chan *Event
	inHeader    bool
	inQuery     bool
	headerLines uint
	queryLines  uint64
	bytesRead   uint64
	lineOffset  uint64
	endOffset   uint64
	stopped     bool
	event       *Event

	// cnmsql: the statement is built here, capped at opt.MaxQueryBytes.
	query     strings.Builder
	truncated bool
	useLine   string
}

// NewParser returns a Parser reading from the open file.
func NewParser(file *os.File, opt Options) *Parser {
	if opt.DefaultLocation == nil {
		opt.DefaultLocation = time.Local
	}
	return &Parser{
		file:      file,
		opt:       opt,
		stopChan:  make(chan bool, 1),
		eventChan: make(chan *Event),
		bytesRead: opt.StartOffset,
		event:     NewEvent(),
	}
}

// Events returns the unbuffered channel events are sent on.
func (p *Parser) Events() <-chan *Event {
	return p.eventChan
}

// Stop stops the parser before parsing the next event or while blocked on
// sending the current event.
func (p *Parser) Stop() {
	p.stopChan <- true
}

// Start parses until EOF, an error or Stop, sending events on Events, which
// it closes when it returns. The file is not closed.
func (p *Parser) Start() error {
	if p.opt.StartOffset > 0 {
		if _, err := p.file.Seek(int64(p.opt.StartOffset), io.SeekStart); err != nil {
			return err
		}
	}

	defer close(p.eventChan)

	r := bufio.NewReader(p.file)

SCANNER_LOOP:
	for !p.stopped {
		select {
		case <-p.stopChan:
			p.stopped = true
			break SCANNER_LOOP
		default:
		}

		line, err := r.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				return err
			}
			break SCANNER_LOOP
		}

		lineLen := uint64(len(line))
		p.bytesRead += lineLen
		p.lineOffset = p.bytesRead - lineLen

		// Filter out meta lines:
		//   /usr/local/bin/mysqld, Version: 5.6.15-62.0-tokudb-7.1.0-tokudb-log (binary). started with:
		//   Tcp port: 3306  Unix socket: /var/lib/mysql/mysql.sock
		//   Time                 Id Command    Argument
		if lineLen >= 20 && ((line[0] == '/' && line[lineLen-6:lineLen] == "with:\n") ||
			(line[0:5] == "Time ") ||
			(line[0:4] == "Tcp ") ||
			(line[0:4] == "TCP ")) {
			continue
		}

		// PMM-1834: Filter out empty comments and MariaDB explain:
		if line == "#\n" || strings.HasPrefix(line, "# explain:") {
			continue
		}

		line = line[0 : lineLen-1]

		if p.inHeader {
			p.parseHeader(line)
		} else if p.inQuery {
			p.parseQuery(line)
		} else if headerRe.MatchString(line) {
			p.inHeader = true
			p.inQuery = false
			p.parseHeader(line)
		}
	}

	if !p.stopped && p.queryLines > 0 {
		p.endOffset = p.bytesRead
		p.sendEvent(false, false)
	}

	return nil
}

func (p *Parser) parseHeader(line string) {
	if !headerRe.MatchString(line) {
		p.inHeader = false
		p.inQuery = true
		p.parseQuery(line)
		return
	}

	if p.headerLines == 0 {
		p.event.Offset = p.lineOffset
	}
	p.headerLines++

	switch {
	case strings.HasPrefix(line, "# Time"):
		m := timeRe.FindStringSubmatch(line)
		if len(m) == 2 {
			p.event.Ts, _ = time.ParseInLocation("060102 15:04:05", m[1], p.opt.DefaultLocation)
		} else {
			m = timeNewRe.FindStringSubmatch(line)
			if len(m) != 2 {
				return
			}
			p.event.Ts, _ = time.ParseInLocation(time.RFC3339Nano, m[1], p.opt.DefaultLocation)
		}
		if userRe.MatchString(line) {
			m := userRe.FindStringSubmatch(line)
			p.event.User = m[1]
			p.event.Host = m[2]
		}
	case strings.HasPrefix(line, "# User"):
		m := userRe.FindStringSubmatch(line)
		if len(m) < 3 {
			return
		}
		p.event.User = m[1]
		p.event.Host = m[2]
	case strings.HasPrefix(line, "# admin"):
		p.parseAdmin(line)
	default:
		p.parseMetrics(line)
	}
}

func (p *Parser) parseMetrics(line string) {
	if submatch := schema.FindStringSubmatch(line); len(submatch) == 2 {
		p.event.Db = submatch[1]
	}

	for _, smv := range metricsRe.FindAllStringSubmatch(line, -1) {
		// [String, Metric, Value], e.g. ["Query_time: 2", "Query_time", "2"]
		switch {
		case strings.HasSuffix(smv[1], "_time") || strings.HasSuffix(smv[1], "_wait"):
			val, _ := strconv.ParseFloat(smv[2], 64)
			p.event.TimeMetrics[smv[1]] = val
		case smv[2] == "Yes" || smv[2] == "No":
			p.event.BoolMetrics[smv[1]] = smv[2] == "Yes"
		case smv[1] == "Schema":
			p.event.Db = smv[2]
		case smv[1] == "Log_slow_rate_type":
			p.event.RateType = smv[2]
		case smv[1] == "Log_slow_rate_limit":
			val, _ := strconv.ParseUint(smv[2], 10, 64)
			p.event.RateLimit = uint(val)
		default:
			val, _ := strconv.ParseUint(smv[2], 10, 64)
			p.event.NumberMetrics[smv[1]] = val
		}
	}
}

// startsEntry reports whether line begins the next entry. cnmsql: upstream
// matched any "# <Capital>" line, which also matches a SQL comment line inside
// a statement and cut the statement there.
func startsEntry(line string) bool {
	return strings.HasPrefix(line, "# Time:") || strings.HasPrefix(line, "# User@Host:")
}

func (p *Parser) parseQuery(line string) {
	if strings.HasPrefix(line, "# admin") {
		p.parseAdmin(line)
		return
	} else if startsEntry(line) {
		p.inHeader = true
		p.inQuery = false
		p.endOffset = p.lineOffset
		p.sendEvent(true, false)
		p.parseHeader(line)
		return
	}

	isUse := useRe.FindString(line)
	switch {
	case p.queryLines == 0 && isUse != "":
		db := strings.TrimPrefix(line, isUse)
		db = strings.TrimRight(db, ";")
		db = strings.Trim(db, "`")
		p.event.Db = db
		// A "use" with no statement after it is the statement itself.
		p.useLine = line
	case setRe.MatchString(line):
	default:
		if p.queryLines > 0 {
			p.appendQuery("\n")
		}
		p.appendQuery(line)
		p.queryLines++
	}
}

// appendQuery adds s to the statement, up to opt.MaxQueryBytes, cutting on a
// UTF-8 boundary. cnmsql: upstream concatenated strings.
func (p *Parser) appendQuery(s string) {
	if p.truncated {
		return
	}
	if limit := p.opt.MaxQueryBytes; limit > 0 {
		if room := limit - p.query.Len(); len(s) > room {
			for room > 0 && !utf8.RuneStart(s[room]) {
				room--
			}
			s = s[:max(room, 0)]
			p.truncated = true
		}
	}
	p.query.WriteString(s)
}

func (p *Parser) parseAdmin(line string) {
	p.event.Admin = true
	m := adminRe.FindStringSubmatch(line)
	p.query.Reset()
	p.query.WriteString(strings.TrimSuffix(m[1], ";"))
	p.queryLines = 1
	// Admin commands are the last line of an event.
	p.endOffset = p.bytesRead
	p.sendEvent(false, false)
}

func (p *Parser) sendEvent(inHeader bool, inQuery bool) {
	p.event.OffsetEnd = p.endOffset
	switch {
	case p.queryLines > 0:
		p.event.Query = strings.TrimSuffix(p.query.String(), ";")
	case p.useLine != "":
		p.event.Query = strings.TrimSuffix(p.useLine, ";")
	}
	p.event.QueryTruncated = p.truncated
	p.event.Db = strings.TrimSuffix(p.event.Db, ";\n")

	_, hasQueryTime := p.event.TimeMetrics["Query_time"]
	event := p.event

	p.event = NewEvent()
	p.headerLines = 0
	p.queryLines = 0
	p.query.Reset()
	p.truncated = false
	p.useLine = ""
	p.inHeader = inHeader
	p.inQuery = inQuery

	// cnmsql: upstream panicked on an event without Query_time and no header
	// lines; such an event (a partial entry) is dropped like any other event
	// without Query_time.
	if !hasQueryTime {
		return
	}

	select {
	case p.eventChan <- event:
	case <-p.stopChan:
		p.stopped = true
	}
}
