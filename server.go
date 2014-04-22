package main

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"github.com/marksamman/mta-json-api/gtfs-realtime"
)

// MTA API Key, from http://datamine.mta.info/
const APIKey = ``

type XMLCharData struct {
	Value string `xml:",chardata"`
}

type XMLLine struct {
	Name   XMLCharData `xml:"name"`
	Status XMLCharData `xml:"status"`
	Text   XMLCharData `xml:"text,omitempty"`
	Time   XMLCharData `xml:"Time,omitempty"`
	Date   XMLCharData `xml:"Date,omitempty"`
}

type XMLSubway struct {
	Lines []XMLLine `xml:"line"`
}

type XMLService struct {
	XMLName xml.Name  `xml:"service"`
	Subway  XMLSubway `xml:"subway"`
}

type JSONLine struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Text   string `json:"text,omitempty"`
	Time   string `json:"time,omitempty"`
	Date   string `json:"date,omitempty"`
}

type JSONDestination struct {
	StopId    string `json:"stop_id"`
	Status    string `json:"status,omitempty"`
	Arrival   int64  `json:"arrival"`
	Departure int64  `json:"departure"`
}

type JSONTrain struct {
	Id           uint64            `json:"id"`
	RouteId      string            `json:"route_id"`
	Status       string            `json:"status,omitempty"`
	Destinations []JSONDestination `json:"destinations"`
}

type Stop struct {
	Name      string  `json:"name"`
	Latitude  float32 `json:"lat"`
	Longitude float32 `json:"lng"`
}

func parseStopsFile(stopsJSON *string) error {
	file, err := os.Open("data/stops.txt")
	if err != nil {
		return err
	}
	defer file.Close()

	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return err
	}

	stops := make(map[string]Stop)
	for _, v := range records {
		lat, _ := strconv.ParseFloat(v[4], 32)
		lng, _ := strconv.ParseFloat(v[5], 32)
		stops[v[0]] = Stop{v[2], float32(lat), float32(lng)}
	}

	res, err := json.Marshal(stops)
	if err != nil {
		return err
	}
	*stopsJSON = fmt.Sprintf("%s", res)
	return nil
}

func updateServiceStatus(serviceStatusJSON *string, mutex *sync.RWMutex) error {
	resp, err := http.Get("http://mta.info/status/serviceStatus.txt")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	xmlService := &XMLService{}

	decoder := xml.NewDecoder(resp.Body)
	if err := decoder.Decode(xmlService); err != nil {
		return err
	}

	lines := []JSONLine{}
	for _, line := range xmlService.Subway.Lines {
		lines = append(lines, JSONLine{line.Name.Value, line.Status.Value, line.Text.Value, line.Time.Value, line.Date.Value})
	}

	res, err := json.Marshal(lines)
	if err != nil {
		return err
	}

	mutex.Lock()
	*serviceStatusJSON = fmt.Sprintf("%s", res)
	mutex.Unlock()
	return nil
}

func serviceStatusUpdater(serviceStatusJSON *string, mutex *sync.RWMutex) {
	ticker := time.NewTicker(15 * time.Second)
	for {
		<-ticker.C
		if err := updateServiceStatus(serviceStatusJSON, mutex); err != nil {
			log.Print(err)
		}
	}
}

func realtimeFeedUpdater(realtimeFeedJSON *string, mutex *sync.RWMutex) {
	ticker := time.NewTicker(2 * time.Minute)
	for {
		<-ticker.C
		if err := updateRealtimeFeed(realtimeFeedJSON, mutex); err != nil {
			log.Print(err)
		}
	}
}

func updateRealtimeFeed(realtimeFeedJSON *string, mutex *sync.RWMutex) error {
	// Feed ID:
	// 0, 2 or empty	= L train
	// 1 				= 1, 2, 3, 4, 5 and 6 train
	resp, err := http.Get("http://datamine.mta.info/mta_esi.php?feed_id=1&key=" + APIKey)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// Parse with ProtoBuf
	feedMessage := &transit_realtime.FeedMessage{}
	if err := proto.Unmarshal(data, feedMessage); err != nil {
		return err
	}

	trains := []JSONTrain{}
	for _, v1 := range feedMessage.GetEntity() {
		destinations := []JSONDestination{}

		for _, v2 := range v1.GetTripUpdate().GetStopTimeUpdate() {
			arrival := v2.GetArrival().GetTime()
			departure := v2.GetDeparture().GetTime()
			if departure == 0 {
				departure = arrival
			}

			destinations = append(destinations, JSONDestination{
				v2.GetStopId(),                                  // stop id
				fmt.Sprintf("%s", v2.GetScheduleRelationship()), // status
				arrival,   // arrival
				departure, // depart
			})
		}

		if len(destinations) != 0 {
			id, _ := strconv.ParseUint(v1.GetId(), 10, 32)
			trains = append(trains, JSONTrain{
				id,
				v1.GetTripUpdate().GetTrip().GetRouteId(),
				fmt.Sprintf("%s", v1.GetVehicle().GetCurrentStatus()),
				destinations,
			})
		}
	}

	res, err := json.Marshal(trains)
	if err != nil {
		return err
	}

	mutex.Lock()
	*realtimeFeedJSON = fmt.Sprintf("%s", res)
	mutex.Unlock()
	return nil
}

func main() {
	serviceStatusJSON, realtimeFeedJSON, stopsJSON := "[]", "[]", "[]"

	var serviceStatusMutex, realtimeFeedMutex sync.RWMutex

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		if err := updateServiceStatus(&serviceStatusJSON, &serviceStatusMutex); err != nil {
			log.Print(err)
		}
		wg.Done()
	}()
	go func() {
		if err := updateRealtimeFeed(&realtimeFeedJSON, &realtimeFeedMutex); err != nil {
			log.Print(err)
		}
		wg.Done()
	}()
	wg.Wait()

	go serviceStatusUpdater(&serviceStatusJSON, &serviceStatusMutex)
	go realtimeFeedUpdater(&realtimeFeedJSON, &realtimeFeedMutex)

	if err := parseStopsFile(&stopsJSON); err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/service_status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*") // Allow external AJAX requests

		serviceStatusMutex.RLock()
		fmt.Fprint(w, serviceStatusJSON)
		serviceStatusMutex.RUnlock()
	})
	http.HandleFunc("/realtime_feed", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*") // Allow external AJAX requests

		realtimeFeedMutex.RLock()
		fmt.Fprint(w, realtimeFeedJSON)
		realtimeFeedMutex.RUnlock()
	})
	http.HandleFunc("/stops", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*") // Allow external AJAX requests

		fmt.Fprint(w, stopsJSON)
	})
	log.Fatal(http.ListenAndServe("localhost:4000", nil))
}
