package aof

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"redis-in-go/internal/info"
)

var AOFInfo map[string]string

func initAOFInfo() {
	AOFInfo = make(map[string]string)
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

		err := os.MkdirAll(dirPathFull, os.FileMode(defaultPermission))
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

		_, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Println("Failed to create appendonly file:", err)
			return
		}

		aofMainfestFileName := aofName + ".manifest"

		aofManifestNameFull := filepath.Join(dirPathFull, aofMainfestFileName)

		_, err = os.Stat(aofManifestNameFull)
		if errors.Is(err, os.ErrNotExist) {
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
}

func ConfigGet(option string) (string, error) {
	value, exists := AOFInfo[option]
	if !exists {
		return "", fmt.Errorf("No such option")
	}
	return value, nil
}

func GetActiveIncrementalFileName() string {
	aofDir, exists := AOFInfo["appenddirname"]
	if !exists {
		fmt.Println("Failed to retrieve aof dir name")
		return ""
	}

	aofFileName, exists := AOFInfo["appendfilename"]
	if !exists {
		fmt.Println("Failed to retrieve aof dir name")
		return ""
	}
	aofFileName = aofFileName + ".manifest"

	manifestFileNameFull := filepath.Join(info.WorkDir, aofDir, aofFileName)

	file, err := os.Open(manifestFileNameFull)
	if err != nil {
		fmt.Println("Failed to open manifest file:", err)
		return ""
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	var manifestTargetLine string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				if strings.Contains(line, "type i") {
					manifestTargetLine = line
				}
				break
			}
			fmt.Println("Failed to read aof manifest:", err)
			return ""
		}
		if strings.Contains(line, "type i") {
			manifestTargetLine = line
			break
		}
	}

	if manifestTargetLine == "" {
		fmt.Println("Failed to find active incremental file")
		return ""
	}

	activeIncrementalFileName, err := extractFileNameFromManifestLine(manifestTargetLine)
	if err != nil {
		fmt.Println(err)
		return ""
	}

	return activeIncrementalFileName
}

func extractFileNameFromManifestLine(line string) (string, error) {
	fields := strings.Fields(line)
	if len(fields) < 2 || fields[0] != "file" {
		return "", fmt.Errorf("Invalid manifest line format")
	}
	return fields[1], nil
}

func GetActiveIncrementalFile() (*os.File, error) {
	fileNameToFlushTo := GetActiveIncrementalFileName()
	aofDir, exists := AOFInfo["appenddirname"]
	if !exists {
		return nil, fmt.Errorf("No directory for aof set")
	}
	fileNameToFlushToFullPath := filepath.Join(info.WorkDir, aofDir, fileNameToFlushTo)

	file, err := os.OpenFile(fileNameToFlushToFullPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("Failed to open active incr file: %s", err)
	}
	return file, nil
}
