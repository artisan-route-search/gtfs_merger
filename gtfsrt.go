package main

import (
	"fmt"
	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	proto "github.com/golang/protobuf/proto"
	"io/ioutil"
	"log"
	"net/http"
)

func main() {
	var (
		username = "YOUR_ACCESS_KEY"
		password = "YOUR_SECRET_KEY"
	)

	client := &http.Client{}
	req, err := http.NewRequest("GET", "http://www3.unobus.co.jp/GTFS/GTFS_RT-VP.bin", nil)
	req.SetBasicAuth(username, password)
	resp, err := client.Do(req)
	defer resp.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	feed := gtfs.FeedMessage{}
	err = proto.Unmarshal(body, &feed)
	if err != nil {
		log.Fatal(err)
	}

	for _, entity := range feed.Entity {
		vehicle := entity.Vehicle
		pos := vehicle.Position
		fmt.Println(
			*entity.Id,
			*vehicle.Trip.TripId,
			*pos.Latitude,
			*pos.Longitude)
	}
}
