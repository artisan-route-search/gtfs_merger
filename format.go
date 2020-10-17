package main

import (
	"fmt"
	"encoding/csv"
	"os"
)

var fields map[string][]string

func load_table(path string,table_name string){
	file, err := os.Open(path + "/" + table_name + ".txt")
	if err != nil {
		fmt.Println("["+path + "/" + table_name + ".txt] is not found")
		return
		// panic(err)
	}
	defer file.Close()

	counter := -1
	reader := csv.NewReader(file)
	titles := map[string]int{}

	for {
		counter++
		line, er := reader.Read()
		if er != nil {
			break
		}
		if counter==0{
			for k,v := range line{
				titles[v]=k
			}
			for _,v := range fields[table_name]{
				if _,ok := titles[v]; !ok{
					titles[v] = len(line)
				}
			}
			continue
		}

		// Load records
		line = append(line,"")

		for _,v := range fields[table_name]{
			fmt.Print(line[titles[v]]," ")
		}
		fmt.Println("")
	}
}

func main(){

	fields = map[string][]string{}
	table_names := []string{"agency","routes","trips","stops","stop_times","calendar","calendar_dates","fare_rules","fare_attributes","shapes","translations","feed_info","frequencies","transfers"}

	fields["agency"]					= []string{"agency_id","agency_name","agency_url","agency_timezone","agency_lang","agency_phone","agency_fare_url","agency_email"}
	fields["routes"]					= []string{"route_id","agency_id","route_short_name","route_long_name","route_desc","route_type","route_url","route_color","route_text_color"}
	fields["trips"]						= []string{"trip_id","route_id","service_id","trip_headsign","trip_short_name","directon_id","block_id","shape_id","wheelchair_accesible","bikes_allowed"}
	fields["stops"]						= []string{"stop_id","stop_code","stop_name","stop_desc","stop_lat","stop_lon","zone_id","stop_url","location_type","parent_station","stop_timezone","wheelchair_boarding","platform_code"}
	fields["stop_times"]			= []string{"trip_id","arrival_time","depature_time","stop_id","stop_sequence","stop_headsign","pickup_type","timepoint"}
	fields["calendar"]				= []string{"service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date"}
	fields["calendar_dates"]  = []string{"service_id","date","exception_type"}
	fields["fare_rules"]			= []string{"fare_id","route_id","origin_id","destination_id","contains_id"}
	fields["fare_attributes"] = []string{"fare_id","price","currency_type","payment_method","transfer"}
	fields["shapes"]  				= []string{"shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence","shape_dist_traveled"}
	fields["translations"]  	= []string{"trans_id","lang","translation"}
	fields["feed_info"]  			= []string{"feed_publisher_name","feed_publisher_url","feed_lang","feed_start_date","feed_end_date","feed_version"}
	fields["frequencies"]  		= []string{"trip_id","start_time","end_time","headway_secs","exact_times"}
	fields["transfers"]  			= []string{"from_stop_id","to_stop_id","transfer_type","min_transfer_time"}

	fmt.Println("start")

	path := "gtfs"

	// load_table(path,"frequencies")
	// return

	for _,table_name := range table_names{
		load_table(path,table_name)
	}
}