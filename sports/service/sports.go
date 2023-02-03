package service

import (
	"EntainTest/sports/db"
	"EntainTest/sports/proto/sports"
	"golang.org/x/net/context"
)

type Sports interface {
	// ListEvents will return a collection of sports.
	ListEvents(ctx context.Context, in *sports.ListEventsRequest) (*sports.ListEventsResponse, error)

	// ListEvents will return a collection of sports.
	GetEvent(ctx context.Context, in *sports.GetEventRequest) (*sports.GetEventResponse, error)
}

// sportsService implements the sports interface.
type sportsService struct {
	sportsRepo db.SportsRepo
}

// NewSportsService instantiates and returns a new sportsService.
func NewSportsService(sportsRepo db.SportsRepo) Sports {
	return &sportsService{sportsRepo}
}

func (s *sportsService) ListEvents(ctx context.Context, in *sports.ListEventsRequest) (*sports.ListEventsResponse, error) {
	event, err := s.sportsRepo.List(in.Filter)
	if err != nil {
		return nil, err
	}

	return &sports.ListEventsResponse{Events: event}, nil
}

func (s *sportsService) GetEvent(ctx context.Context, in *sports.GetEventRequest) (*sports.GetEventResponse, error) {
	event, err := s.sportsRepo.GetEvent(in)
	if err != nil {
		return nil, err
	}

	return &sports.GetEventResponse{Event: event}, nil
}
