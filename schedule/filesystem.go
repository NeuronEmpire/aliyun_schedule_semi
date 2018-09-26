package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func MakeDirIfNotExists(filePath string) (err error) {
	fmt.Println("MakeDirIfNotExists", filePath)
	dir := filepath.Dir(filePath)
	dirInfo, err := os.Stat(dir)
	if dirInfo == nil || os.IsNotExist(err) {
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	return nil
}
