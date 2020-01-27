package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"code.sajari.com/docconv"
	"gopkg.in/jdkato/prose.v2"
)

var punctuation = []string{".", "!", "?", ":", ";", ",", "(", ")", "—", "·"}

// InvIndex - mapping from word to all the document names it occurs in
type InvIndex map[string]StringSet

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
func CreateInvIndex(dir string) InvIndex {
	invIndex := make(InvIndex)

	files, err := filepath.Glob(dir + "/*") // ioutil.ReadDir("./texts/")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Indexing %v documents.", len(files))

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

		log.Printf("%v: Found %v tokens.", file, len(doc.Tokens()))
		// Iterate over the doc's tokens:
		for _, token := range doc.Tokens() {
			if !IsIn(punctuation, token.Text) {
				if _, ok := invIndex[token.Text]; !ok {
					invIndex[token.Text] = make(StringSet)
				}

				invIndex[token.Text].Add(file)
			}
		}
	}

	return invIndex
}

// Count - returns the number of entries in InvIndex
func (invIndex InvIndex) Count() int { return len(invIndex) }

// Find - searches for the given token in an inverse index
func (invIndex InvIndex) Find(token string) []string {
	index := invIndex[token]
	entries := make([]string, len(index))
	i := 0
	for k := range index {
		entries[i] = k
		i++
	}

	return entries
}

// Save - save index to file on disk
func (invIndex InvIndex) Save(file string) {
	encodeFile, err := os.Create(file)
	if err != nil {
		panic(err)
	}

	encoder := gob.NewEncoder(encodeFile)

	if err := encoder.Encode(invIndex); err != nil {
		panic(err)
	}
	encodeFile.Close()
}

// Load - load index from file on disk
func Load(file string) InvIndex {
	// Open a RO file
	decodeFile, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer decodeFile.Close()

	decoder := gob.NewDecoder(decodeFile)

	invIndex := make(InvIndex)

	decoder.Decode(&invIndex)

	return invIndex
}

func main() {
	invIndexFile := "inv-index.gob"

	CreateInvIndex("./texts").Save(invIndexFile)

	invIndex := Load(invIndexFile)

	fmt.Println("Unique tokens count:", invIndex.Count())

	// ==================================

	token := "input"
	entries := invIndex.Find(token)
	fmt.Println(token, ":", strings.Join(entries, ", "))

	token = "был"
	entries = invIndex.Find(token)
	fmt.Println(token, ":", strings.Join(entries, ", "))
}
