package main

import (
	"fmt"
	"os"
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
	err := os.WriteFile(aofDir+"/"+filename, []byte(""), 0644)

	if err != nil {
		fmt.Println(err)
	}

	config.AofIncrFileCount = config.AofIncrFileCount + 1
}