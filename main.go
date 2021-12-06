package main

import (
	"fmt"
	"encoding/csv"
	"os"
	"log"
	"io"
	"sync"
	"flag"
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
	writer.Write(append(fields[table_name],"origin_gtfs"))

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
				
				replace_stop_ids := map[string]string{}
				replace_stop_ids["from_stop_id"] = "stop_id"
				replace_stop_ids["to_stop_id"] = "stop_id"
				replace_stop_ids["parent_station"] = "stop_id"
				replace_stop_ids["origin_id"] = "stop_id"
				replace_stop_ids["destination_id"] = "stop_id"
				if v,ok := replace_stop_ids[column_name];ok{
					column_name = v
				}

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

				if column_name == "feed_lang" || column_name == "agency_lang"{
					str = strings.ToLower(str)
				}
			}
			outline = append(outline,str)
		}
		writer.Write(append(outline,path[8:]))
	}
	writer.Flush()
}

func replace_gtfs_ids(path string){

	replace_ids := map[string]map[string]string{}

	for _,v := range id_column_list{
		replace_ids[v] = map[string]string{}
	}

	for table_name,_ := range fields{
		load_table(path,table_name,replace_ids)
	}

	if err := os.Mkdir("./replace_ids/", 0777); err != nil {
		fmt.Println(err)
	}
	if err := os.Mkdir("./replace_ids/"+path[8:], 0777); err != nil {
		fmt.Println(err)
	}

	// replace_idの出力
	for table_name,original_ids:=range replace_ids{
		f, err := os.Create("./replace_ids/"+path[8:]+"/rep_"+table_name+".csv")
		if err != nil {
			log.Fatal(err)
		}
		w := csv.NewWriter(f)
	
		if err := w.Error(); err != nil {
			log.Fatal(err)
		}
		
		w.Write([]string{"original","new"})
		for o,n:=range original_ids{
			w.Write([]string{o,n})
		}
		w.Flush()
		f.Close()
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
	iswrite := false
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
			iswrite = true
		}
	}
	w.Flush()
	f.Close()
	if !iswrite{
		if err := os.Remove(outname); err != nil {
			fmt.Println(err)
		}
	}
}

func initialization(){
	id_column_list = []string{"agency_id","route_id","trip_id","stop_id","service_id","fare_id","zone_id","shape_id"}
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
	fields["fare_attributes"] = []string{"fare_id","price","currency_type","payment_method","transfers","agency_id","transfer_duration"}
	fields["shapes"]  				= []string{"shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence","shape_dist_traveled"}
	fields["feed_info"]  			= []string{"feed_publisher_name","feed_publisher_url","feed_lang","feed_start_date","feed_end_date","feed_version"}
	fields["frequencies"]  		= []string{"trip_id","start_time","end_time","headway_secs","exact_times"}
	fields["transfers"]  			= []string{"from_stop_id","to_stop_id","transfer_type","min_transfer_time"}
}

func initialization_jp(){
	id_column_list = []string{"agency_id","route_id","trip_id","stop_id","service_id","fare_id","zone_id","shape_id"}
	id_columns	= map[string]bool{}
	for _,v := range id_column_list{
		id_columns[v] = true
	}
	fields 										= map[string][]string{}
	fields["agency"]					= []string{"agency_id","agency_name","agency_url","agency_timezone","agency_lang","agency_phone","agency_fare_url","agency_email"}
	fields["agency_jp"]				= []string{"agency_id","agency_official_name","agency_zip_number","agency_address","agency_president_pos","agency_president_name"}
	fields["routes"]					= []string{"route_id","agency_id","route_short_name","route_long_name","route_desc","route_type","route_url","route_color","route_text_color","jp_parent_route_id"}
	fields["routes_jp"]				= []string{"route_id","route_update_date","origin_stop","via_stop","destination_stop"}
	fields["trips"]						= []string{"trip_id","route_id","service_id","trip_headsign","trip_short_name","directon_id","block_id","shape_id","wheelchair_accesible","bikes_allowed","jp_trip_desc","jp_trip_desc_symbol","jp_office_id"}
	fields["office_jp"]				= []string{"office_id","office_name","office_url","office_phone"}
	fields["stops"]						= []string{"stop_id","stop_code","stop_name","stop_desc","stop_lat","stop_lon","zone_id","stop_url","location_type","parent_station","stop_timezone","wheelchair_boarding","platform_code"}
	fields["stop_times"]			= []string{"trip_id","arrival_time","departure_time","stop_id","stop_sequence","stop_headsign","pickup_type","timepoint"}
	fields["calendar"]				= []string{"service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date"}
	fields["calendar_dates"]  = []string{"service_id","date","exception_type"}
	fields["fare_rules"]			= []string{"fare_id","route_id","origin_id","destination_id","contains_id"}
	fields["fare_attributes"] = []string{"fare_id","price","currency_type","payment_method","transfers","agency_id","transfer_duration"}
	fields["shapes"]  				= []string{"shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence","shape_dist_traveled"}
	fields["translations"]  	= []string{"trans_id","lang","translation"}
	fields["feed_info"]  			= []string{"feed_publisher_name","feed_publisher_url","feed_lang","feed_start_date","feed_end_date","feed_version"}
	fields["frequencies"]  		= []string{"trip_id","start_time","end_time","headway_secs","exact_times"}
	fields["transfers"]  			= []string{"from_stop_id","to_stop_id","transfer_type","min_transfer_time"}
}

func main(){

	expansion := flag.String("e", "", "expansion")
	flag.Parse()

	if *expansion == "jp"{
		initialization_jp()
	} else {
		initialization()
	}

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

	// ZIP
	filePaths, _ := findfiles("./onegtfs")
	zipPath := "./GTFS.zip"
	if err := archive(zipPath, filePaths); err != nil {
		panic(err)
	}
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

// ZIP
func findfiles(targetDir string) ([]string, error) {

	var paths []string
	err := filepath.Walk(targetDir,
			func(path string, info os.FileInfo, err error) error {
					rel, err := filepath.Rel(targetDir, path)
					if err != nil {
							return err
					}

					if info.IsDir() {
							paths = append(paths, fmt.Sprintf("%s/", rel))
							return nil
					}

					paths = append(paths, rel)

					return nil
			})

	if err != nil {
			return nil, err
	}

	return paths, nil
}

func archive(output string, paths []string) error {
	var compressedFile *os.File
	var err error

	//ZIPファイル作成
	if compressedFile, err = os.Create(output); err != nil {
			return err
	}
	defer compressedFile.Close()

	if err := compress(compressedFile, ".", paths); err != nil {
			return err
	}

	return nil
}

func compress(compressedFile io.Writer, targetDir string, files []string) error {
	w := zip.NewWriter(compressedFile)

	for _, filename := range files {
			filepath := fmt.Sprintf("%s/%s", targetDir, "./onegtfs/" + filename)
			info, err := os.Stat(filepath)
			if err != nil {
					return err
			}

			if info.IsDir() {
					continue
			}

			file, err := os.Open(filepath)
			if err != nil {
					return err
			}
			defer file.Close()

			hdr, err := zip.FileInfoHeader(info)
			if err != nil {
					return err
			}

			hdr.Name = filename

			f, err := w.CreateHeader(hdr)
			if err != nil {
					return err
			}

			contents, _ := ioutil.ReadFile(filepath)
			_, err = f.Write(contents)
			if err != nil {
					return err
			}
	}

	if err := w.Close(); err != nil {
			return err
	}

	return nil
}