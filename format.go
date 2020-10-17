package main

import (
	"fmt"
)

type Agency struct{
	Agency_id string
	Agency_name string
	Agency_url string
	Agency_timezone string
	Agency_lang string
	Agency_phone string
	Agency_fare_url string
	Agency_email string
}

type Feed_info struct{
	Feed_publisher_name string
	Feed_publisher_url string
	Feed_lang string
	Feed_start_date string
	Feed_end_date string
	Feed_version string
}

type translations struct{
	Trans_id string
	Lang string
	Translation string
}

type Route struct{
	Route_id string
	Agency_id string
	Route_short_name string
	Route_long_name string
	Route_desc string
	Route_type string
	Route_url string
	Route_color string
	Route_text_color string
}

type Trip struct{
	Trip_id string
	Route_id string
	Service_id string
	Trip_headsign string
	Trip_short_name string
	Directon_id string
	Block_id string
	Shape_id string
	Wheelchair_accesible string
	bikes_allowed string
}

type Frequency struct{
	Trip_id string
	Start_time string
	End_time string
	Headway_secs string
	exact_times string
}

type Calender struct{
	Service_id string
	Monday string
	Tuesday string
	Wednesday string
	Thursday string
	Friday string
	Saturday string
	Sunday string
	start_date string
	end_date string
}

type Shape struct{
	Shape_id string
	Shape_pt_lat string
	Shape_pt_lon string
	Shape_pt_sequence string
	Shape_dist_traveled string
}

type Stop_time struct{
	Trip_id string
	Arrival_time string
	Depature_time string
	Stop_id string
	Stop_sequence string
	Stop_headsign string
	Pickup_type string
	Shape_dist_travel string
	timepoint string
}

type Calendar_date struct{
	Service_id string
	Date string
	Exception_type string
}

type Stop struct{
	Stop_id string
	Stop_code string
	Stop_name string
	Stop_desc string
	Stop_lat string
	Stop_lon string
	Zone_id string
	Stop_url string
	Location_type string
	Parent_station string
	Stop_timezone string
	Wheelchair_boarding string
	platform_code string
}

type Transfer struct{
	From_stop_id string
	To_stop_id string
	Transfer_type string
	Min_transfer_time string
}

type Fare_attributes struct{
	Fare_id string
	Price string
	Currency_type string
	Payment_method string
	Transfer string
	Transfer_duration string
}

type Fare_rule struct{
	Fare_id string
	Route_id string
	Origin_id string
	Destination_id string
	contains_id string
}

func main(){
	fmt.Println("start")
}