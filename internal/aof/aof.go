package aof

import (
	"fmt"
	"os"
	"path/filepath"

	"redis-in-go/internal/info"
)

var (
	AOFInfo                 map[string]string
	ActiveIncrementFileName string
)

func initAOFInfo() {
	AOFInfo = make(map[string]string)
}

func initActiveIncrementalFileName(name string) {
	ActiveIncrementFileName = name
}

func Init(appendonly, appenddirname, appendfilename, appendfsync string) {
	initAOFInfo()
	AOFInfo["appendonly"] = appendonly
	AOFInfo["appenddirname"] = appenddirname
	AOFInfo["appendfilename"] = appendfilename
	AOFInfo["appendfsync"] = appendfsync

	if AOFInfo["appendonly"] == "yes" {
		dirname, exists := AOFInfo["appenddirname"]
		if !exists {
			dirname = "appendonlydir"
		}

		defaultPermission := 0o755
		dirPathFull := filepath.Join(info.WorkDir, dirname)

		err := os.Mkdir(dirPathFull, os.FileMode(defaultPermission))
		if err != nil {
			fmt.Println("Failed to create appendonly directory:", err)
			return
		}

		aofName, exists := AOFInfo["appendfilename"]
		if !exists {
			aofName = "appendonly"
		}

		incrementFactor := "1" // HACK:
		aofNameFull := aofName + "." + incrementFactor + "." + "incr.aof"
		filePath := filepath.Join(dirPathFull, aofNameFull)

		_, err = os.Create(filePath)
		if err != nil {
			fmt.Println("Failed to create appendonly file:", err)
			return
		}

		initActiveIncrementalFileName(aofNameFull)

		aofMainfestFileName := aofName + ".manifest"

		aofManifestNameFull := filepath.Join(dirPathFull, aofMainfestFileName)

		manifestFile, err := os.Create(aofManifestNameFull)
		if err != nil {
			fmt.Println("Failed to create manifest file:", err)
			return
		}
		defer manifestFile.Close()

		seqLine := fmt.Sprintf("seq %s", incrementFactor)
		aofType := "i"
		typeLine := fmt.Sprintf("type %s", aofType)

		_, err = manifestFile.WriteString(fmt.Sprintf("file %s %s %s", aofNameFull, seqLine, typeLine))
		if err != nil {
			fmt.Println("Failed to write to manifest:", err)
			return
		}
	}
}

func ConfigGet(option string) (string, error) {
	value, exists := AOFInfo[option]
	if !exists {
		return "", fmt.Errorf("No such option")
	}
	return value, nil
}

func GetFileNameToFlushAOFTo() string {
	return ActiveIncrementFileName
}
