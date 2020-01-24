package main

import (
	"fmt"
	"log"
	"path/filepath"

	"code.sajari.com/docconv"
	"gopkg.in/jdkato/prose.v2"
)

// InvIndex - mapping from word to all the documents in occurs in
type InvIndex map[string][]string

var punctuation = []string{".", "!", "?", ":", ";", ",", "(", ")", "—", "·"}

// IsIn - checks if the value is in a slice
func IsIn(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

// CreateInvIndex - Create inverse index of words for text files in a directory
func CreateInvIndex(dir string) (index InvIndex) {
	files, err := filepath.Glob(dir + "/*") // ioutil.ReadDir("./texts/")
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		// os.Stats(f)
		res, err := docconv.ConvertPath(file)
		if err != nil {
			log.Fatal(err)
		}

		doc, err := prose.NewDocument(res.Body, prose.WithExtraction(false), prose.WithTagging(false))
		if err != nil {
			log.Fatal(err)
		}

		// Iterate over the doc's tokens:
		for _, tok := range doc.Tokens() {
			if !IsIn(punctuation, tok.Text) {
				fmt.Println(tok.Text)
				// index[tok.Text] = file
			}
		}
	}

	return
}

func main() {
	CreateInvIndex("./texts")
}
