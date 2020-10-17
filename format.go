package main

import (
	"fmt"
	"encoding/csv"
	"os"
	"log"
	"io"
	"sync"
	"bufio"
	"strconv"
	"strings"
	"io/ioutil"
	"path/filepath"
	"archive/zip"
	"github.com/google/uuid"
)

var fields map[string][]string
var id_columns map[string]bool
var id_column_list []string

func newCsvReader(r io.Reader) *csv.Reader {
	br := bufio.NewReader(r)
	bs, err := br.Peek(3)
	if err != nil {
		return csv.NewReader(br)
	}
	if bs[0] == 0xEF && bs[1] == 0xBB && bs[2] == 0xBF {
		br.Discard(3)
	}
	return csv.NewReader(br)
}

func load_table(path string,table_name string,replace_ids map[string]map[string]string){
	// Open read table file
	rf, rerr := os.Open(path + "/" + table_name + ".txt")
	if rerr != nil {
		fmt.Println("["+path + "/" + table_name + ".txt] is not found")
		return
	}
	defer rf.Close()
	reader := newCsvReader(rf)

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
			if str != ""{
				if _,ok := id_columns[column_name];ok{
					if _,ok2 := replace_ids[column_name][str];!ok2{
						uuidObj, _ := uuid.NewUUID()
						uuidstr := uuidObj.String()
						replace_ids[column_name][str] = uuidstr
						str = uuidstr
					} else {
						str = replace_ids[column_name][str]
					}
				}	
			}
			outline = append(outline,str)
		}
		writer.Write(outline)
	}
	writer.Flush()
}

func replace_gtfs_ids(path string){

	table_names 		:= []string{"agency","routes","trips","stops","stop_times","calendar","calendar_dates","fare_rules","fare_attributes","shapes","translations","feed_info","frequencies","transfers"}

	replace_ids := map[string]map[string]string{}

	for _,v := range id_column_list{
		replace_ids[v] = map[string]string{}
	}

	for _,table_name := range table_names{
		load_table(path,table_name,replace_ids)
	}
}

func integration_csvs(file_names []string, outname string){
	f, err := os.Create(outname)
	if err != nil {
		log.Fatal(err)
	}
	w := csv.NewWriter(f)

	if err := w.Error(); err != nil {
			log.Fatal(err)
	}

	file_counter := -1
	for _,path := range file_names{
		file, err := os.Open(path)
		if err != nil {
			continue
		}
		defer file.Close()
		file_counter++

		reader := csv.NewReader(file)
		counter := -1
		for {
			counter++
			line, err := reader.Read()
			if err != nil {
				if counter == 0{
					file_counter--
				}
				break
			}
			if counter == 0 && file_counter != 0{
				continue
			}
			w.Write(line)
		}
	}
	w.Flush()
}

func initialization(){
	id_column_list = []string{"agency_id","route_id","trip_id","stop_id","service_id","fare_id","shape_id","trans_id"}
	id_columns	= map[string]bool{}
	for _,v := range id_column_list{
		id_columns[v] = true
	}
	fields 										= map[string][]string{}
	fields["agency"]					= []string{"agency_id","agency_name","agency_url","agency_timezone","agency_lang","agency_phone","agency_fare_url","agency_email"}
	fields["routes"]					= []string{"route_id","agency_id","route_short_name","route_long_name","route_desc","route_type","route_url","route_color","route_text_color"}
	fields["trips"]						= []string{"trip_id","route_id","service_id","trip_headsign","trip_short_name","directon_id","block_id","shape_id","wheelchair_accesible","bikes_allowed"}
	fields["stops"]						= []string{"stop_id","stop_code","stop_name","stop_desc","stop_lat","stop_lon","zone_id","stop_url","location_type","parent_station","stop_timezone","wheelchair_boarding","platform_code"}
	fields["stop_times"]			= []string{"trip_id","arrival_time","departure_time","stop_id","stop_sequence","stop_headsign","pickup_type","timepoint"}
	fields["calendar"]				= []string{"service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date"}
	fields["calendar_dates"]  = []string{"service_id","date","exception_type"}
	fields["fare_rules"]			= []string{"fare_id","route_id","origin_id","destination_id","contains_id"}
	fields["fare_attributes"] = []string{"fare_id","price","currency_type","payment_method","transfer"}
	fields["shapes"]  				= []string{"shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence","shape_dist_traveled"}
	fields["translations"]  	= []string{"trans_id","lang","translation"}
	fields["feed_info"]  			= []string{"feed_publisher_name","feed_publisher_url","feed_lang","feed_start_date","feed_end_date","feed_version"}
	fields["frequencies"]  		= []string{"trip_id","start_time","end_time","headway_secs","exact_times"}
	fields["transfers"]  			= []string{"from_stop_id","to_stop_id","transfer_type","min_transfer_time"}
}

func main(){

	initialization()

	// Unzip all gtfs
	gtfspaths := []string{}
	paths,_ := dirwalk("./GTFS")
	if err := os.Mkdir("./unzip/", 0777); err != nil {
		fmt.Println(err)
	}
	wg := sync.WaitGroup{}
	for index,path := range paths {
		if(!strings.HasSuffix(path, ".zip")){
			continue
		}
		if err := os.Mkdir("./unzip/"+strconv.Itoa(index), 0777); err != nil {
			fmt.Println(err)
		}
		gtfspaths = append(gtfspaths,"./unzip/"+strconv.Itoa(index))
		wg.Add(1)
		go func(path string,index int){
			defer wg.Done()
			unzip(path,"./unzip/"+strconv.Itoa(index))
			replace_gtfs_ids("./unzip/"+strconv.Itoa(index))
		}(path,index)
	}
	wg.Wait()

	// Merge GTFS
	if err := os.Mkdir("./onegtfs/", 0777); err != nil {
		fmt.Println(err)
	}

	wg.Add(len(fields))
	for k,_ := range fields{
		go func(k string){
			defer wg.Done()
			files := []string{}
			for _,v := range gtfspaths{
				files = append(files,v + "/replace_" + k + ".txt")
			}
			integration_csvs(files,"onegtfs/"+k+".txt")
		}(k)
	}
	wg.Wait()
}

func dirwalk(dir string) ([]string,[]string) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(err)
	}

	var paths,file_names []string
	for _, file := range files {
		paths = append(paths, filepath.Join(dir, file.Name()))
		file_names = append(file_names,file.Name())
	}
	return paths,file_names
}

func unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer rc.Close()

		path := filepath.Join(dest, f.Name)
		if f.FileInfo().IsDir() {
			os.MkdirAll(path, f.Mode())
		} else {
			f, err := os.OpenFile(
					path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
					return err
			}
			defer f.Close()

			_, err = io.Copy(f, rc)
			if err != nil {
					return err
			}
		}
	}
	return nil
}