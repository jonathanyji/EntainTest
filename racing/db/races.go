package db

import (
	"database/sql"
	"github.com/golang/protobuf/ptypes"
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"sync"
	"time"

	"git.neds.sh/matty/entain/racing/proto/racing"
)

// RacesRepo provides repository access to races.
type RacesRepo interface {
	// Init will initialise our races repository.
	Init() error

	// List will return a list of races.
	List(filter *racing.ListRacesRequestFilter) ([]*racing.Race, error)

	// GetRace will return a race by Id.
	GetRace(req *racing.GetRaceRequest) (*racing.Race, error)
}

type racesRepo struct {
	db   *sql.DB
	init sync.Once
}

// NewRacesRepo creates a new races repository.
func NewRacesRepo(db *sql.DB) RacesRepo {
	return &racesRepo{db: db}
}

// Init prepares the race repository dummy data.
func (r *racesRepo) Init() error {
	var err error

	r.init.Do(func() {
		// For test/example purposes, we seed the DB with some dummy races.
		err = r.seed()
	})

	return err
}

func (r *racesRepo) GetRace(req *racing.GetRaceRequest) (*racing.Race, error){
	var (
		err   error
		query string
	)

    id := req.GetId()

    // Retrieve the race using the id
	query = getRaceQuery(id)

	row := r.db.QueryRow(query)
	if err != nil {
		return nil, err
	}
	
	return r.scanSingleRace(row)
}

func (r *racesRepo) List(filter *racing.ListRacesRequestFilter) ([]*racing.Race, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getRaceQueries()[racesList]

	query, args = r.applyFilter(query, filter)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return r.scanRaces(rows)
}

func (r *racesRepo) applyFilter(query string, filter *racing.ListRacesRequestFilter) (string, []interface{}) {
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
		orderType:= mapRacesToDbName(filter.OrderType)
		orderQuery:= " ORDER BY " + orderType
		query += orderQuery
	}

	return query, args
}

func (m *racesRepo) scanRaces(
	rows *sql.Rows,
) ([]*racing.Race, error) {
	var races []*racing.Race
	today := time.Now()

	for rows.Next() {
		var race racing.Race
		var advertisedStart time.Time

		if err := rows.Scan(&race.Id, &race.MeetingId, &race.Name, &race.Number, &race.Visible, &advertisedStart); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}

			return nil, err
		}

		ts, err := ptypes.TimestampProto(advertisedStart)
		if err != nil {
			return nil, err
		}

		race.AdvertisedStartTime = ts

		if (advertisedStart.After(today)){
			race.Status = "OPEN"
		} else {
			race.Status = "CLOSED"
		}

		races = append(races, &race)
	}

	return races, nil
}

func mapRacesToDbName(orderType string) string {
	race := map[string]string{
		"meetingID": "meeting_id",
		"name": "name",
		"number": "number",
		"visible": "visible",
		"start": "advertised_start_time",
	}
	return race[orderType]
}

func (m *racesRepo) scanSingleRace(row *sql.Row)(*racing.Race, error){
	today := time.Now()
	var race racing.Race
	var advertisedStart time.Time

		if err := row.Scan(&race.Id, &race.MeetingId, &race.Name, &race.Number, &race.Visible, &advertisedStart); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}

			return nil, err
		}

		ts, err := ptypes.TimestampProto(advertisedStart)
		if err != nil {
			return nil, err
		}

		race.AdvertisedStartTime = ts

		if (advertisedStart.After(today)){
			race.Status = "OPEN"
		} else {
			race.Status = "CLOSED"
		}

		return &race, nil
}
