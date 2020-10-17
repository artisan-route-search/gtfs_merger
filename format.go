package main

import (
	"fmt"
	"encoding/csv"
	"os"
	"log"
	"github.com/google/uuid"
)

var fields map[string][]string
var id_columns map[string]bool
var replace_ids map[string]map[string]string

func load_table(path string,table_name string){
	// Open read table file
	rf, rerr := os.Open(path + "/" + table_name + ".txt")
	if rerr != nil {
		fmt.Println("["+path + "/" + table_name + ".txt] is not found")
		return
	}
	defer rf.Close()
	reader := csv.NewReader(rf)

	// Open write table file
	wf, werr := os.Create(path + "/replace_" + table_name + ".txt")
	if werr != nil {
		log.Fatal(werr)
	}
	defer wf.Close()
	writer := csv.NewWriter(wf)
	writer.Write(fields[table_name])

	counter := -1
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
		outline := []string{}

		for _,column_name := range fields[table_name]{
			str := line[titles[column_name]]

			// Replace id column
			if _,ok := id_columns[column_name];ok{
				if _,ok := replace_ids[column_name][str];!ok{
					uuidObj, _ := uuid.NewUUID()
					uuidstr := uuidObj.String()
					replace_ids[column_name][str] = uuidstr
					str = uuidstr
				} else {
					str = replace_ids[column_name][str]
				}
			}

			// fmt.Print(str," ")
			outline = append(outline,str)
		}
		writer.Write(outline)
	}
	writer.Flush()
}

func main(){

	fields = map[string][]string{}

	table_names 		:= []string{"agency","routes","trips","stops","stop_times","calendar","calendar_dates","fare_rules","fare_attributes","shapes","translations","feed_info","frequencies","transfers"}
	id_column_list	:= []string{"agency_id","route_id","trip_id","stop_id","service_id","fare_id","shape_id","trans_id"}

	replace_ids = map[string]map[string]string{}
	id_columns	= map[string]bool{}

	for _,v := range id_column_list{
		id_columns[v] = true
		replace_ids[v] = map[string]string{}
	}

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