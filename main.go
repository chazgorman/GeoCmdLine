package main

import (
	"fmt"
	"io/ioutil"
	"encoding/xml"
	"os"
	"log"
	"strings"
	"strconv"
	"net/http"
	"github.com/tidwall/gjson"
	"encoding/csv"
	"sort"
)

type FieldType struct {
	name string
	fieldtype string
	alias string
	length int
}

type AppConfig struct {
	DataUrl             string `xml:"dataUrl"`
	LayerNumbers        string `xml:"layerNumbers"`
	OutputJsonDirectory string `xml:"outputJsonDirectory"`
	FileLoggingEnabled  bool `xml:"fileLoggingEnabled"`
	PromptForRun        bool `xml:"promptForRun"`
	LayerNamesList      string `xml:"layerNamesList"`
}

var logFileName string = "log.txt"
var appLog *log.Logger
var config AppConfig

func main() {
	xmlFile, err := ioutil.ReadFile("./config/config2.xml")
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	xml.Unmarshal(xmlFile, &config)

	if config.FileLoggingEnabled {
		fmt.Println("File Logging Enabled")

		os.Remove(logFileName)

		file, err := os.OpenFile("log.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			fmt.Println("Failed to create log file")
			return
		}
		appLog = log.New(file, "app", log.Lshortfile)
	} else {
		appLog = log.New(os.Stderr,"app", log.Lshortfile)
	}

	appLog.Println("Starting app")

	if config.PromptForRun {
		os.Stderr.WriteString("Press enter to start...")
		var input string
		fmt.Scanln(&input)
	}

	appLog.Println("Running app")

	appLog.Println("Reading layer names from configuration file")
	layerNamesEn := strings.Split(config.LayerNamesList, ",")

	var queryUrlEng = config.DataUrl

	createOutputDirectories(config)

	appLog.Println("Getting Json layer data for layers")
	getLayers(queryUrlEng, layerNamesEn, config.OutputJsonDirectory)

	appLog.Println("Converting Json layer data to CSV")
	convertToCSV(config.OutputJsonDirectory)

}

func createOutputDirectories(config AppConfig){
	appLog.Println("Creating output directories")

	err := os.Mkdir(config.OutputJsonDirectory,os.ModeDir)
	if err != nil {
		appLog.Println("Error creating english json directory.", err)
	}
}

func getLayers(queryUrl string, layerNames []string, outputDir string){
	layerConfigs := strings.Split(config.LayerNumbers,",")
	layers := make([]string,0)

	for _, lyrConfig := range layerConfigs {
		if strings.Contains(lyrConfig,"-"){
			rangeLayers := strings.Split(lyrConfig,"-")

			appLog.Println(rangeLayers)

			start, _ := strconv.ParseInt(rangeLayers[0],10, 64)
			end, _ := strconv.ParseInt(rangeLayers[1], 10, 64)

			var layer int64 = 0
			for layer = start; layer <= end; layer++ {
				appLog.Println(layer)
				layerStr := strconv.FormatInt(layer,10)
				layers = append(layers,layerStr)
			}
		} else {
			layers = append(layers,lyrConfig)
		}
	}

	appLog.Println(layers)

	for _, layer := range layers {
		intCurrentLayer, _ := strconv.ParseInt(layer,10, 64)
		requestUrl := queryUrl + "/" + layer + "/query?where=1=1&f=json&outFields=*"

		appLog.Println("Requesting Json layer data at URL: " + requestUrl)

		resp, err := http.Get(requestUrl)
		if err != nil {
			appLog.Println("Error making request to URL: ", requestUrl)
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()

		filePath := ""
		if intCurrentLayer == 0 {
			filePath = outputDir + "/" + layerNames[intCurrentLayer] + ".json"
		} else {
			filePath = outputDir + "/" + layerNames[intCurrentLayer - 1] + ".json"
		}
		ioutil.WriteFile(filePath,body,os.ModePerm)
	}
}

func convertToCSV(targetDir string){
	jsonFileDir := targetDir
	csvDir := targetDir + "\\csv"

	appLog.Println("Output CSV to directory: ", csvDir)

	appLog.Println("Creating output CSV directory")

	rmerr := os.RemoveAll(csvDir)
	if rmerr != nil {
		appLog.Println("Error removing CSV directory.", rmerr)
	}

	err := os.Mkdir(csvDir,os.ModeDir)
	if err != nil {
		appLog.Println("Error creating CSV directory.", err)
	}

	files, err := ioutil.ReadDir(jsonFileDir)
	if err != nil {
		appLog.Fatal(err)
	}

	for _, file := range files {
		if file.IsDir() == false {
			outputFile := csvDir + "\\" + strings.Replace(file.Name(),"json","csv", 1)
			appLog.Println(outputFile)
			convertJsonFile(jsonFileDir + "\\" + file.Name(), outputFile)
		}
	}
}

func convertJsonFile(filePath string, outputFilePath string){

	appLog.Println("Converting file: ", filePath)

	json, err := ioutil.ReadFile(filePath)
	if err != nil {
		appLog.Println("Error converting file to Csv: ", err)
		return
	}
	jsonString := string(json)
	jsonResult := gjson.Get(jsonString,"fields")

	fieldsArray := jsonResult.Array()

	// Array to hold field names
	fields := []string{}
	// Map from field name to type
	fieldTypes := make(map[string]string)
	// Map from field index to field name
	fieldTypesIndex := make(map[int]string)


	fieldIndex := 0
	var fieldKeys []int

	for _, result := range fieldsArray {
		fieldName := gjson.Get(result.Raw,"name")
		fields = append(fields,fieldName.Str)

		fieldType := gjson.Get(result.Raw, "type")
		fieldTypes[fieldName.Str] = fieldType.Str

		fieldTypesIndex[fieldIndex] = fieldName.Str
		fieldKeys = append(fieldKeys, fieldIndex)
		fieldIndex++
	}

	sort.Ints(fieldKeys)

	records := [][]string{
		fields,
	}

	jsonFeatureResult := gjson.Get(jsonString, "features.#.attributes")

	jsonFeatureResult.ForEach(func(key, attValue gjson.Result) bool{
		record := []string{}

		for i := range fieldKeys {
			k,_ := fieldTypesIndex[i]

			v := fieldTypes[k]

			value := gjson.Get(attValue.Raw,k)

			valueString := ""

			switch v {
			case "esriFieldTypeString","esriFieldTypeDate","esriFieldTypeGUID":
				valueString = value.Str
				break
			case "esriFieldTypeBlob","esriFieldTypeRaster","esriFieldTypeXML","esriFieldTypeGeometry":
				valueString = ""
				break
			case "esriFieldTypeInteger","esriFieldTypeSmallInteger","esriFieldTypeOID","esriFieldTypeGlobalID":
				valueString = strconv.FormatInt(value.Int(),10)
				break
			case "esriFieldTypeDouble","esriFieldTypeSingle":
				valueString = strconv.FormatFloat(value.Num,'f',5,64)
				break
			default:
				valueString = ""
				break
			}

			record = append(record, strings.TrimSpace(valueString))
		}

		records = append(records,record)
		return true // keep iterating
	})

	outputFile, err := os.Create(outputFilePath) // os.OpenFile(outputFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		appLog.Println("Failed to create output CSV file", err)
		return
	}
	defer outputFile.Close()

	w := csv.NewWriter(outputFile)

	for _, record := range records {
		if err := w.Write(record); err != nil {
			appLog.Fatalln("error writing record to csv:", err)
		}
	}

	// Write any buffered data to the underlying writer (standard output).
	w.Flush()

	if err := w.Error(); err != nil {
		appLog.Fatal(err)
	}
}
