package parser

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const postfixForm = "Jan 2 15:04:05 2006"

type LogReader interface {
	ParseLogFile()
	GetAllRecords() []MailEvent
	GetRecords(time.Duration) []MailEvent
}

type LogStore struct {
	Filename string
	Events   []MailEvent
}

type MailEvent interface {
	GetQID() string
	GetComponent() string
	GetEventTime() time.Time
	GetRecordType() int
}

type SMTPdConnectionRecord struct {
	Component  string
	DateTime   time.Time
	ProcessId  int
	RecordType int
	Sequence   int
	ClientName string
	ClientIP   string
	Hostname   string
	QueueID    string
}

func (r *SMTPdConnectionRecord) GetQID() string {
	return ""
}

func (r *SMTPdConnectionRecord) GetComponent() string {
	return r.Component
}
func (r *SMTPdConnectionRecord) GetRecordType() int {
	return r.RecordType
}

func (r *SMTPdConnectionRecord) GetEventTime() time.Time {
	return r.DateTime
}

// smtpd
func readSMTPdLine(l string) (MailEvent, error) {
	var rec SMTPdConnectionRecord
	parts := strings.Split(l, "]: ")
	meta := parts[0]
	message := parts[1]
	meta_parts := strings.Fields(meta)
	rec.Hostname = meta_parts[3]
	rec.Component = "smtpd"
	year := time.Now().Year()
	cur_year := fmt.Sprintf("%d", year)
	dateparts := append(meta_parts[:3], cur_year)
	ts, err := time.Parse(postfixForm, strings.Join(dateparts, " "))
	if err != nil {
		return &rec, err
	}
	rec.DateTime = ts
	mparts := strings.Fields(message)

	if strings.Contains(l, "connect from") {
		if mparts[0] == "disconnect" {
			rec.RecordType = 1
		} else if mparts[0] == "connect" {
			rec.RecordType = 0
		}
		hparts := strings.Split(mparts[2], "[")
		hname := hparts[0]
		ip := strings.Trim(hparts[1], "]")
		rec.ClientIP = ip
		rec.ClientName = hname
	} else if strings.Contains(message, "lost connection after CONNECT") {
		rec.RecordType = 2
	} else if strings.Contains(message, "client=") {
		rec.RecordType = 3
		rec.QueueID = strings.Trim(mparts[0], ":")
	}
	return &rec, nil
}

// pickup
type PickupRecord struct {
	Component  string
	DateTime   time.Time
	ProcessId  int
	RecordType int
	Sequence   int
	Hostname   string
	QueueID    string
	Uid        string
	From       string
}

func (r *PickupRecord) GetQID() string {
	return ""
}

func (r *PickupRecord) GetComponent() string {
	return r.Component
}
func (r *PickupRecord) GetRecordType() int {
	return r.RecordType
}

func (r *PickupRecord) GetEventTime() time.Time {
	return r.DateTime
}

func parsePickupLine(l string) (MailEvent, error) {
	var rec PickupRecord
	parts := strings.Split(l, "]: ")
	meta := parts[0]
	message := parts[1]
	meta_parts := strings.Fields(meta)
	rec.Hostname = meta_parts[3]
	rec.Component = "pickup"
	year := time.Now().Year()
	cur_year := fmt.Sprintf("%d", year)
	dateparts := append(meta_parts[:3], cur_year)
	ts, err := time.Parse(postfixForm, strings.Join(dateparts, " "))
	if err != nil {
		return &rec, err
	}
	rec.DateTime = ts
	mparts := strings.Fields(message)
	rec.QueueID = strings.Trim(mparts[0], ":")
	rec.Uid = strings.Split(mparts[1], "=")[1]
	return &rec, err
}

// cleanup
type CleanupRecord struct {
	Component  string
	DateTime   time.Time
	ProcessId  int
	RecordType int
	Sequence   int
	Hostname   string
	QueueID    string
	MessageId  string
}

func (r *CleanupRecord) GetQID() string {
	return ""
}
func (r *CleanupRecord) GetComponent() string {
	return r.Component
}
func (r *CleanupRecord) GetRecordType() int {
	return r.RecordType
}
func (r *CleanupRecord) GetEventTime() time.Time {
	return r.DateTime
}

func parseCleanupLine(l string) (MailEvent, error) {
	var rec CleanupRecord
	parts := strings.Split(l, "]: ")
	meta := parts[0]
	message := parts[1]
	meta_parts := strings.Fields(meta)
	rec.Hostname = meta_parts[3]
	rec.Component = "cleanup"
	year := time.Now().Year()
	cur_year := fmt.Sprintf("%d", year)
	dateparts := append(meta_parts[:3], cur_year)
	ts, err := time.Parse(postfixForm, strings.Join(dateparts, " "))
	if err != nil {
		return &rec, err
	}
	rec.DateTime = ts
	mparts := strings.Fields(message)
	rec.QueueID = strings.Trim(mparts[0], ":")
	mid := strings.Split(mparts[1], "=")[1]
	rec.MessageId = strings.Trim(mid, "<>")
	//fmt.Printf("%+v\n", rec)
	return &rec, err
}

// qmgr
type QueueManagerRecord struct {
	Component  string
	DateTime   time.Time
	ProcessId  int
	RecordType int
	Sequence   int
	Hostname   string
	QueueID    string
	From       string
	Size       int64
	Nrcpt      int64
}

func (r *QueueManagerRecord) GetQID() string {
	return ""
}
func (r *QueueManagerRecord) GetComponent() string {
	return r.Component
}
func (r *QueueManagerRecord) GetRecordType() int {
	return r.RecordType
}
func (r *QueueManagerRecord) GetEventTime() time.Time {
	return r.DateTime
}

func parseQmgrLine(l string) (MailEvent, error) {
	var rec QueueManagerRecord
	parts := strings.Split(l, "]: ")
	meta := parts[0]
	message := parts[1]
	meta_parts := strings.Fields(meta)
	rec.Hostname = meta_parts[3]
	rec.Component = "qmgr"
	year := time.Now().Year()
	cur_year := fmt.Sprintf("%d", year)
	dateparts := append(meta_parts[:3], cur_year)
	ts, err := time.Parse(postfixForm, strings.Join(dateparts, " "))
	if err != nil {
		return &rec, err
	}
	rec.DateTime = ts
	mparts := strings.Fields(message)
	rec.QueueID = strings.Trim(mparts[0], ":")

	if mparts[1] == "removed" {
		rec.Sequence = 1
		return &rec, nil
	}

	tsize := strings.Trim(mparts[2], ",")
	rec.Size, err = strconv.ParseInt(strings.Split(tsize, "=")[1], 0, 0)
	if err != nil {
		return &rec, err
	}
	tcnt := strings.Trim(mparts[3], ",")
	rec.Nrcpt, err = strconv.ParseInt(strings.Split(tcnt, "=")[1], 0, 0)
	if err != nil {
		return &rec, err
	}
	//fmt.Printf("%+v\n", rec)
	return &rec, err
}

// smtp
type SMTPRecord struct {
	Component     string
	DateTime      time.Time
	ProcessId     int
	RecordType    int
	Sequence      int
	Hostname      string
	QueueID       string
	To            string
	Relay         string
	Delay         int64
	Status        string
	StatusMessage string
}

func (r *SMTPRecord) GetQID() string {
	return r.QueueID
}

func (r *SMTPRecord) GetComponent() string {
	return r.Component
}
func (r *SMTPRecord) GetRecordType() int {
	return r.RecordType
}

func (r *SMTPRecord) GetEventTime() time.Time {
	return r.DateTime
}

func parseSMTPLine(l string) (MailEvent, error) {
	var rec SMTPRecord
	parts := strings.Split(l, "]: ")
	meta := parts[0]
	message := parts[1]
	meta_parts := strings.Fields(meta)
	rec.Hostname = meta_parts[3]
	rec.Component = "smtp"
	year := time.Now().Year()
	cur_year := fmt.Sprintf("%d", year)
	dateparts := append(meta_parts[:3], cur_year)
	ts, err := time.Parse(postfixForm, strings.Join(dateparts, " "))
	if err != nil {
		return &rec, nil
	}
	rec.DateTime = ts
	qsplit := strings.SplitN(message, ": ", 2)
	mparts := strings.Split(qsplit[1], ", ")

	mcomponents := make(map[string]string)
	for _, p := range mparts {
		kvpair := strings.Split(p, "=")
		if len(kvpair) == 2 {
			k := kvpair[0]
			v := kvpair[1]
			mcomponents[k] = v
		}
	}

	rec.QueueID = strings.Trim(mparts[0], ":")
	if rec.QueueID == "warning" {
		return &rec, nil
	} else if rec.QueueID == "connect" {
		return &rec, nil
	}

	rec.To = mcomponents["to"]
	relay := mcomponents["relay"]
	delay := mcomponents["delay"]
	status_split := strings.SplitN(mcomponents["status"], " ", 2)
	rec.Status = status_split[0]
	rec.StatusMessage = status_split[1]
	rec.Delay, err = strconv.ParseInt(delay, 0, 0)
	if err != nil {
		return &rec, nil
	}
	if strings.Contains(relay, "[") {
		rec.Relay = strings.Split(relay, "[")[1]
	} else {
		rec.Relay = relay
	}
	if err != nil {
		return &rec, nil
	}
	return &rec, nil
}

// local
type DeliveryRecord struct {
	Component       string
	DateTime        time.Time
	ProcessId       int
	RecordType      int
	Sequence        int
	Hostname        string
	QueueID         string
	To              string
	OriginalTo      string
	Relay           string
	Delay           float64
	Delays          string
	Status          string
	DSN             string
	DeliveryMessage string
}

func (r *DeliveryRecord) GetQID() string {
	return r.QueueID
}

func (r *DeliveryRecord) GetComponent() string {
	return r.Component
}
func (r *DeliveryRecord) GetRecordType() int {
	return r.RecordType
}

func (r *DeliveryRecord) GetEventTime() time.Time {
	return r.DateTime
}

func parseDeliveryLine(l string) (MailEvent, error) {
	var rec DeliveryRecord
	now := time.Now()
	year := now.Year()

	parts := strings.Split(l, "]: ")
	meta := parts[0]
	message := parts[1]
	meta_parts := strings.Fields(meta)
	rec.Hostname = meta_parts[3]
	rec.Component = "local"
	cur_year := fmt.Sprintf("%d", year)
	dateparts := append(meta_parts[:3], cur_year)
	ts, err := time.Parse(postfixForm, strings.Join(dateparts, " "))
	if err != nil {
		return &rec, err
	}
	rec.DateTime = ts
	mparts := strings.Fields(message)
	rec.QueueID = strings.Trim(mparts[0], ":")
	if rec.QueueID == "warning" {
		return &rec, err
	} else if rec.QueueID == "connect" {
		return &rec, err
	}

	dmr := strings.Split(message, "status=")[1]
	dm := strings.Trim(strings.SplitN(dmr, " ", 2)[1], "()")
	rec.DeliveryMessage = dm
	rec.To = strings.Trim(strings.Split(strings.Trim(mparts[1], "<>,"), "=")[1], "<>")
	rec.OriginalTo = strings.Trim(strings.Split(strings.Trim(mparts[2], "<>,"), "=")[1], "<>")
	relay := strings.Split(strings.Trim(mparts[3], "],"), "=")[1]
	delay := strings.Split(strings.Trim(mparts[4], ","), "=")[1]
	rec.Delays = strings.Split(strings.Trim(mparts[5], ","), "=")[1]
	rec.DSN = strings.Split(strings.Split(mparts[6], " ")[0], "=")[1]
	rec.Status = strings.Split(strings.Split(mparts[7], " ")[0], "=")[1]
	rec.Delay, err = strconv.ParseFloat(delay, 32)
	if err != nil {
		return &rec, err
	}
	if strings.Contains(relay, "[") {
		rec.Relay = strings.Split(relay, "[")[1]
	} else {
		rec.Relay = relay
	}
	if err != nil {
		return &rec, err
	}
	return &rec, err
}

func (l *LogStore) ParseLogFile() {
	mlog, err := os.Open(l.Filename)
	if err != nil {
		log.Print("unable to open log file")
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(mlog)
	var event MailEvent
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, " postfix/") {
			//log.Print("Line doesn't appear to contain a postfix identifier")
			continue
		}
		allfields := strings.Fields(line)
		c1 := strings.Split(allfields[4], "[")
		component := c1[0]
		switch component {
		case "postfix/smtpd":
			event, err = readSMTPdLine(line)
		case "postfix/pickup":
			event, err = parsePickupLine(line)
		case "postfix/cleanup":
			event, err = parseCleanupLine(line)
		case "postfix/qmgr":
			event, err = parseQmgrLine(line)
		case "postfix/smtp":
			event, err = parseSMTPLine(line)
		case "postfix/local":
			event, err = parseDeliveryLine(line)
		}
		l.Events = append(l.Events, event)
	}
}

func (l *LogStore) GetRecords(ma time.Duration) []MailEvent {
	var elist []MailEvent
	for _, e := range l.Events {
		if time.Since(e.GetEventTime()) < ma {
			elist = append(elist, e)
		} else {
		}
	}
	return elist
}
