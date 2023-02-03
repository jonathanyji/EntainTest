package db

import (
	"database/sql"
	"github.com/golang/protobuf/ptypes"
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"sync"
	"time"

	"EntainTest/sports/proto/sports"
)

// SportsRepo provides repository access to sports.
type SportsRepo interface {
	// Init will initialise our sports repository.
	Init() error

	// List will return a list of sports.
	List(filter *sports.ListEventsRequestFilter) ([]*sports.Event, error)

	// GetRace will return a race by Id.
	GetEvent(req *sports.GetEventRequest) (*sports.Event, error)
}

type sportsRepo struct {
	db   *sql.DB
	init sync.Once
}

// NewSportsRepo creates a new sports repository.
func NewSportsRepo(db *sql.DB) SportsRepo {
	return &sportsRepo{db: db}
}

// Init prepares the race repository dummy data.
func (r *sportsRepo) Init() error {
	var err error

	r.init.Do(func() {
		// For test/example purposes, we seed the DB with some dummy sports.
		err = r.seed()
	})

	return err
}

func (r *sportsRepo) GetEvent(req *sports.GetEventRequest) (*sports.Event, error){
	var (
		err   error
		query string
	)

    id := req.GetId()

    // Retrieve the race using the id
	query = getEventQuery(id)

	row := r.db.QueryRow(query)
	if err != nil {
		return nil, err
	}
	
	return r.scanSingleEvent(row)
}

func (r *sportsRepo) List(filter *sports.ListEventsRequestFilter) ([]*sports.Event, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getEventQueries()[eventList]

	query, args = r.applyFilter(query, filter)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return r.scanEvents(rows)
}

func (r *sportsRepo) applyFilter(query string, filter *sports.ListEventsRequestFilter) (string, []interface{}) {
	var (
		clauses []string
		args    []interface{}
	)

	if filter == nil {
		return query, args
	}

	if filter.Visible {
		clauses = append(clauses, "visible = ?")
		args = append(args, filter.Visible)
	}

	if len(filter.MeetingIds) > 0 {
		clauses = append(clauses, "meeting_id IN ("+strings.Repeat("?,", len(filter.MeetingIds)-1)+"?)")

		for _, meetingID := range filter.MeetingIds {
			args = append(args, meetingID)
		}
	}

	if len(clauses) != 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}

	if filter.OrderType != "" {
		orderType:= mapEventsToDbName(filter.OrderType)
		orderQuery:= " ORDER BY " + orderType
		query += orderQuery
	}

	return query, args
}

func (m *sportsRepo) scanEvents(
	rows *sql.Rows,
) ([]*sports.Event, error) {
	var sports_arry []*sports.Event
	today := time.Now()

	for rows.Next() {
		var sport sports.Event
		var advertisedStart time.Time

		if err := rows.Scan(&sport.Id, &sport.MeetingId, &sport.Name, &sport.Number, &sport.Visible, &advertisedStart); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}

			return nil, err
		}

		ts, err := ptypes.TimestampProto(advertisedStart)
		if err != nil {
			return nil, err
		}

		sport.AdvertisedStartTime = ts

		if (advertisedStart.After(today)){
			sport.Status = "OPEN"
		} else {
			sport.Status = "CLOSED"
		}

		sports_arry = append(sports_arry, &sport)
	}

	return sports_arry, nil
}

func mapEventsToDbName(orderType string) string {
	race := map[string]string{
		"meetingID": "meeting_id",
		"name": "name",
		"number": "number",
		"visible": "visible",
		"start": "advertised_start_time",
	}
	return race[orderType]
}

func (m *sportsRepo) scanSingleEvent(row *sql.Row)(*sports.Event, error){
	today := time.Now()
	var sport sports.Event
	var advertisedStart time.Time

		if err := row.Scan(&sport.Id, &sport.MeetingId, &sport.Name, &sport.Number, &sport.Visible, &advertisedStart); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}

			return nil, err
		}

		ts, err := ptypes.TimestampProto(advertisedStart)
		if err != nil {
			return nil, err
		}

		sport.AdvertisedStartTime = ts

		if (advertisedStart.After(today)){
			sport.Status = "OPEN"
		} else {
			sport.Status = "CLOSED"
		}

		return &sport, nil
}
