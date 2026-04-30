package main

import (
	"fmt"
	"os"
	"bufio"
	"path/filepath"
)

func createAofDir() (string, error) {
	aofDir := filepath.Join(config.Dir, config.AppendDirName)
	if err := os.MkdirAll(aofDir, 0755); err != nil {
		return "", fmt.Errorf("createAofDir: %w", err)
	}
	return aofDir, nil
}

func createEmptyIncFile(aofDir string) {
    filename := fmt.Sprintf("%s.%d.incr.aof", config.AppendFilename, config.AofIncrFileCount)
	
    if err := os.WriteFile(filepath.Join(aofDir, filename), []byte(""), 0644); err != nil {
        fmt.Println(err)
		os.Exit(1)
    }
	
	appendManifestEntry(aofDir, filename, config.AofIncrFileCount)
	config.AofIncrFileCount++
}

func appendManifestEntry(aofDir string, filename string, seq int) error {
    manifestPath := filepath.Join(aofDir, fmt.Sprintf("%s.manifest", config.AppendFilename))
    line := fmt.Sprintf("file %s seq %d type i", filename, seq)
    return appendLine(manifestPath, line)
}

func createManifestFile(aofDir string) {
	filename := fmt.Sprintf("%s.manifest", config.AppendFilename)
	writeToFile(aofDir+"/"+filename, "")
}

func writeToFile(filepath string, contents string) {
	err := os.WriteFile(filepath, []byte(contents), 0644)
	if err != nil {
		fmt.Println(err)
	}
}

func appendLine(path string, line string) error {
    file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return err
    }
    defer file.Close()

    writer := bufio.NewWriter(file)
    if _, err := writer.WriteString(line + "\n"); err != nil {
        return err
    }
    return writer.Flush()
}