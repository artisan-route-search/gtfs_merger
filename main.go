package main

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	csvtag "github.com/artonge/go-csv-tag/v2"
	"github.com/google/uuid"
)

type Table struct {
	Name 		string
	Columns []string
}

var fields []Table	// GTFSに含まれるファイル一覧
var id_columns map[string]bool	// uuidで置き換えるIDリスト

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

func GetTableColumns(tables []Table,name string)[]string{
	for _,v:=range tables{
		if v.Name == name{
			return v.Columns
		}
	}
	return []string{}
}

func load_table(path string, table_name string, replace_ids map[string]map[string]string, replaceColumns map[string]string) {
	// Open read table file
	rf, rerr := os.Open(path + "/" + table_name + ".txt")
	if rerr != nil {
		fmt.Println("[" + path + "/" + table_name + ".txt] is not found")
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

	tableColumns := GetTableColumns(fields,table_name)

	writer := csv.NewWriter(wf)
	writer.Write(append(tableColumns, "origin_gtfs"))

	counter := -1
	titles := map[string]int{}

	for {
		counter++
		line, er := reader.Read()
		if er != nil {
			break
		}
		if counter == 0 {
			for k, v := range line {
				titles[v] = k
			}
			for _, v := range tableColumns {
				if _, ok := titles[v]; !ok {
					titles[v] = len(line)
				}
			}
			continue
		}

		// Load records
		line = append(line, "")
		outline := []string{}

		for _, column_name := range tableColumns {
			str := line[titles[column_name]]

			// Replace id column
			if str != "" {

				// 運賃情報の場合、stop_idとzone_idのどちらか判定する
				if column_name == "origin_id" || column_name == "destination_id"{
					if v,ok := replace_ids["zone_id"][str];ok{
						// zone_idを使って置き換え
						str = v
						fmt.Println("use zone_id")
					} else if v,ok:= replace_ids["stop_id"][str];ok{
						// stop_idを使って置き換え
						str = v
						fmt.Println("use stop_id")
					} else {
						fmt.Println(str+" is not found in both stop_ids and zone_ids")
					}

				// 運賃情報以外では置き換え対象の場合のみ置き換える
				} else {
					if v, ok := replaceColumns[column_name]; ok {
						column_name = v
					}
	
					if _, ok := id_columns[column_name]; ok {
						if _, ok2 := replace_ids[column_name][str]; !ok2 {
							// まだ置き換えたことがないので置き換え
							if str == "0" {
								replace_ids[column_name][str] = str
							} else {
								uuidObj, _ := uuid.NewUUID()
								uuidstr := uuidObj.String()
								replace_ids[column_name][str] = uuidstr
								if column_name == "zone_id" {
									fmt.Println(column_name+" is",str,"->",uuidstr)
								}
								str = uuidstr	
							}
						} else {
							str = replace_ids[column_name][str]
						}
					}
	
					if column_name == "feed_lang" || column_name == "agency_lang" {
						str = strings.ToLower(str)
					}
				}
			}
			outline = append(outline, str)
		}
		writer.Write(append(outline, path[8:]))
	}
	writer.Flush()
}

// // 運賃情報がstop_idにより記載されていればstop_id、zone_idにより記載されていればzone_idを返す
// func FareIdBase(){}

func replace_gtfs_ids(path string) {

	replace_ids := map[string]map[string]string{}

	for k, _ := range id_columns {
		replace_ids[k] = map[string]string{}
	}

	replaceColumns := map[string]string{}
	replaceColumns["from_stop_id"] = "stop_id"
	replaceColumns["to_stop_id"] = "stop_id"
	replaceColumns["parent_station"] = "stop_id"

	// 運賃情報がstop_idベースの場合
	replaceColumns["origin_id"] = "stop_id"
	replaceColumns["destination_id"] = "stop_id"

	// // 運賃情報がzone_idベースの場合
	// replaceColumns["origin_id"] = "zone_id"
	// replaceColumns["destination_id"] = "zone_id"

	for _, v := range fields {
		load_table(path, v.Name, replace_ids, replaceColumns)
	}

	if err := os.Mkdir("./replace_ids/", 0777); err != nil {
		fmt.Println(err)
	}
	if err := os.Mkdir("./replace_ids/"+path[8:], 0777); err != nil {
		fmt.Println(err)
	}

	// replace_idの出力
	for table_name, original_ids := range replace_ids {
		f, err := os.Create("./replace_ids/" + path[8:] + "/rep_" + table_name + ".csv")
		if err != nil {
			log.Fatal(err)
		}
		w := csv.NewWriter(f)

		if err := w.Error(); err != nil {
			log.Fatal(err)
		}

		w.Write([]string{"original", "new"})
		for o, n := range original_ids {
			w.Write([]string{o, n})
		}
		w.Flush()
		f.Close()
	}
}

func integration_csvs(file_names []string, outname string) {
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
	for _, path := range file_names {
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
				if counter == 0 {
					file_counter--
				}
				break
			}
			if counter == 0 && file_counter != 0 {
				continue
			}
			w.Write(line)
			iswrite = true
		}
	}
	w.Flush()
	f.Close()
	if !iswrite {
		if err := os.Remove(outname); err != nil {
			fmt.Println(err)
		}
	}
}

func initialization() {
	id_column_list := []string{"agency_id", "route_id", "trip_id", "stop_id", "service_id", "fare_id", "zone_id", "shape_id"}
	id_columns = map[string]bool{}
	for _, v := range id_column_list {
		id_columns[v] = true
	}
	fields = append(fields, Table{
		Name: "agency",
		Columns: []string{"agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang", "agency_phone", "agency_fare_url", "agency_email"},
	})
	fields = append(fields, Table{
		Name: "routes",
		Columns: []string{"route_id", "agency_id", "route_short_name", "route_long_name", "route_desc", "route_type", "route_url", "route_color", "route_text_color"},
	})
	fields = append(fields, Table{
		Name: "trips",
		Columns: []string{"trip_id", "route_id", "service_id", "trip_headsign", "trip_short_name", "directon_id", "block_id", "shape_id", "wheelchair_accesible", "bikes_allowed"},
	})
	fields = append(fields, Table{
		Name: "stops",
		Columns: []string{"stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon", "zone_id", "stop_url", "location_type", "parent_station", "stop_timezone", "wheelchair_boarding", "platform_code"},
	})
	fields = append(fields, Table{
		Name: "stop_times",
		Columns: []string{"trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "stop_headsign", "pickup_type", "timepoint"},
	})
	fields = append(fields, Table{
		Name: "calendar",
		Columns: []string{"service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"},
	})
	fields = append(fields, Table{
		Name: "calendar_dates",
		Columns: []string{"service_id", "date", "exception_type"},
	})
	fields = append(fields, Table{
		Name: "shapes",
		Columns: []string{"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence", "shape_dist_traveled"},
	})
	fields = append(fields, Table{
		Name: "feed_info",
		Columns: []string{"feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_start_date", "feed_end_date", "feed_version"},
	})
	fields = append(fields, Table{
		Name: "frequencies",
		Columns: []string{"trip_id", "start_time", "end_time", "headway_secs", "exact_times"},
	})
	fields = append(fields, Table{
		Name: "transfers",
		Columns: []string{"from_stop_id", "to_stop_id", "transfer_type", "min_transfer_time"},
	})
	fields = append(fields, Table{
		Name: "translations",
		Columns: []string{"trans_id", "lang", "translation"},
	})
	fields = append(fields, Table{
		Name: "fare_rules",
		Columns: []string{"fare_id", "route_id", "origin_id", "destination_id", "contains_id"},
	})
	fields = append(fields, Table{
		Name: "fare_attributes",
		Columns: []string{"fare_id", "price", "currency_type", "payment_method", "transfers", "agency_id", "transfer_duration"},
	})
}

func initialization_jp() {
	id_column_list := []string{"agency_id", "route_id", "trip_id", "stop_id", "service_id", "fare_id", "zone_id", "shape_id"}
	id_columns = map[string]bool{}
	for _, v := range id_column_list {
		id_columns[v] = true
	}
	fields = append(fields, Table{
		Name: "agency",
		Columns: []string{"agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang", "agency_phone", "agency_fare_url", "agency_email"},
	})
	fields = append(fields, Table{
		Name: "agency_jp",
		Columns: []string{"agency_id", "agency_official_name", "agency_zip_number", "agency_address", "agency_president_pos", "agency_president_name"},
	})
	fields = append(fields, Table{
		Name: "routes",
		Columns: []string{"route_id", "agency_id", "route_short_name", "route_long_name", "route_desc", "route_type", "route_url", "route_color", "route_text_color", "jp_parent_route_id"},
	})
	fields = append(fields, Table{
		Name: "routes_jp",
		Columns: []string{"route_id", "route_update_date", "origin_stop", "via_stop", "destination_stop"},
	})
	fields = append(fields, Table{
		Name: "trips",
		Columns: []string{"trip_id", "route_id", "service_id", "trip_headsign", "trip_short_name", "directon_id", "block_id", "shape_id", "wheelchair_accesible", "bikes_allowed", "jp_trip_desc", "jp_trip_desc_symbol", "jp_office_id"},
	})
	fields = append(fields, Table{
		Name: "office_jp",
		Columns: []string{"office_id", "office_name", "office_url", "office_phone"},
	})
	fields = append(fields, Table{
		Name: "stops",
		Columns: []string{"stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon", "zone_id", "stop_url", "location_type", "parent_station", "stop_timezone", "wheelchair_boarding", "platform_code"},
	})
	fields = append(fields, Table{
		Name: "stop_times",
		Columns: []string{"trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "stop_headsign", "pickup_type", "timepoint"},
	})
	fields = append(fields, Table{
		Name: "calendar",
		Columns: []string{"service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"},
	})
	fields = append(fields, Table{
		Name: "calendar_dates",
		Columns: []string{"service_id", "date", "exception_type"},
	})
	fields = append(fields, Table{
		Name: "shapes",
		Columns: []string{"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence", "shape_dist_traveled"},
	})
	fields = append(fields, Table{
		Name: "translations",
		Columns: []string{"trans_id", "lang", "translation"},
	})
	fields = append(fields, Table{
		Name: "feed_info",
		Columns: []string{"feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_start_date", "feed_end_date", "feed_version"},
	})
	fields = append(fields, Table{
		Name: "frequencies",
		Columns: []string{"trip_id", "start_time", "end_time", "headway_secs", "exact_times"},
	})
	fields = append(fields, Table{
		Name: "transfers",
		Columns: []string{"from_stop_id", "to_stop_id", "transfer_type", "min_transfer_time"},
	})
	fields = append(fields, Table{
		Name: "fare_rules",
		Columns: []string{"fare_id", "route_id", "origin_id", "destination_id", "contains_id"},
	})
	fields = append(fields, Table{
		Name: "fare_attributes",
		Columns: []string{"fare_id", "price", "currency_type", "payment_method", "transfers", "agency_id", "transfer_duration"},
	})
}

type Translation2 struct {
	TransID string `csv:"trans_id"`
	Lang string `csv:"lang"`
	Translation string `csv:"translation"`
}

type Translation3 struct {
	TransID string `csv:"table_name"`
	// FieldName string `csv:"field_name"`
	Language string `csv:"language"`
	Translation string `csv:"translation"`
	// RecordID string `csv:"record_id"`
	// RecordSubID string `csv:"record_sub_id"`
	FieldValue string `csv:"field_value"`
}

func main() {

	expansion := flag.String("e", "", "expansion")
	flag.Parse()

	// GTFSかGTFS-JPかの判定
	if *expansion == "jp" {
		initialization_jp()
	} else {
		initialization()
	}

	// Unzip all gtfs
	gtfspaths := []string{}
	paths, _ := dirwalk("./GTFS")
	if err := os.Mkdir("./unzip/", 0777); err != nil {
		fmt.Println(err)
	}

	// GTFS毎にファイルを置き換えていく
	wg := sync.WaitGroup{}
	for index, path := range paths {
		if !strings.HasSuffix(path, ".zip") {
			continue
		}
		dir := "./unzip/"+strconv.Itoa(index)
		if err := os.Mkdir(dir, 0777); err != nil {
			fmt.Println(err)
		}
		gtfspaths = append(gtfspaths, dir)
		wg.Add(1)
		go func(originalPath string, destinationPath string) {
			defer wg.Done()

			// GTFSの展開
			unzip(originalPath, destinationPath)

			// 運賃情報をzone_idベースかstop_idベースか判定

			// IDの置き換え
			replace_gtfs_ids(destinationPath)

			// translationsの仕様によっては片合わせ
			translationsColomns := getColumons(destinationPath + "/translations.txt")
			if _,ok:=translationsColomns["trans_id"];ok{
				if _,ok:=translationsColomns["lang"];ok{
					if _,ok:=translationsColomns["translation"];ok{
						// GTFS-JP 第二版仕様
						fmt.Println("translations.txt is version 2.")
					}
				}
			}
			if _,ok:=translationsColomns["language"];ok{
				if _,ok:=translationsColomns["field_name"];ok{
					if _,ok:=translationsColomns["field_value"];ok{
						fmt.Println("translations.txt is version 3.")
						// GTFS-JP 第三版仕様
						// 第二版仕様にする処理
						translations2 := []Translation2{}
						translations3 := []Translation3{}

						csvtag.LoadFromPath(destinationPath + "/translations.txt",&translations3)
						for _,v := range translations3 {
							translations2 = append(translations2, Translation2{
								TransID: v.FieldValue,
								Translation: v.Translation,
								Lang: v.Language,
							})
						}
						if err := csvtag.DumpToFile(translations2,destinationPath + "/translations2.txt");err != nil {
							log.Fatalln(err)
						}
					}
				}
			}
		}(path, dir)
	}
	wg.Wait()

	// Merge GTFS
	if err := os.Mkdir("./onegtfs/", 0777); err != nil {
		fmt.Println(err)
	}

	wg.Add(len(fields))
	for _, v := range fields {
		go func(k string) {
			defer wg.Done()
			files := []string{}
			for _, v := range gtfspaths {
				files = append(files, v+"/replace_"+k+".txt")
			}
			integration_csvs(files, "onegtfs/"+k+".txt")
		}(v.Name)
	}
	wg.Wait()

	// ZIP
	filePaths, _ := findfiles("./onegtfs")
	zipPath := "./GTFS.zip"
	if err := archive(zipPath, filePaths); err != nil {
		panic(err)
	}
}

func dirwalk(dir string) ([]string, []string) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(err)
	}

	var paths, file_names []string
	for _, file := range files {
		paths = append(paths, filepath.Join(dir, file.Name()))
		file_names = append(file_names, file.Name())
	}
	return paths, file_names
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
		filepath := fmt.Sprintf("%s/%s", targetDir, "./onegtfs/"+filename)
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

func getColumons(filePath string)(translationColumns map[string]bool){
	translationColumns = map[string]bool{}

	fp, err := os.Open(filePath)
	if err != nil {
		// エラー処理
	}
	defer fp.Close()

	scanner := bufio.NewScanner(fp)

	for scanner.Scan() {
		// ここで一行ずつ処理
		cols := strings.Split(scanner.Text(),",")
		for _,col := range cols {
			translationColumns[col] = true
		}
		break
	}

	if err = scanner.Err(); err != nil {
		return translationColumns
	}
	return translationColumns
}
