package db

const (
	eventList = "list"
)

func getEventQueries() map[string]string {
	return map[string]string{
		eventList: `
			SELECT 
				id, 
				meeting_id, 
				name, 
				number, 
				visible, 
				advertised_start_time 
			FROM events
		`,
	}
}

func getEventQuery(id string) string {
	return `
			SELECT 
				id, 
				meeting_id, 
				name, 
				number, 
				visible, 
				advertised_start_time 
			FROM events
			WHERE id = 
		` + id
}
