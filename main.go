package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"code.sajari.com/docconv"
	mapset "github.com/deckarep/golang-set"
	bitset "github.com/willf/bitset"
	"gopkg.in/jdkato/prose.v2"
)

var punctuation = []string{".", "!", "?", ":", ";", ",", "(", ")", "—", "·"}

// InvIndex - mapping from tokens to all the document names it occurs in
type InvIndex map[string]mapset.Set

// IncMatrix - incidence matrix of tokens and documents
type IncMatrix map[string]bitset.BitSet

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
func CreateInvIndex(dir string) (InvIndex, mapset.Set) {
	invIndex := make(InvIndex)

	files, err := filepath.Glob(dir + "/*") // ioutil.ReadDir("./texts/")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Indexing %v documents.", len(files))

	fullSet := mapset.NewSet()
	for _, file := range files {
		fullSet.Add(file)

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
					invIndex[token.Text] = mapset.NewSet()
				}

				invIndex[token.Text].Add(file)
			}
		}
	}

	return invIndex, fullSet
}

// CreateIncMatrix - Create incidence matrix of words for text files in a directory
func CreateIncMatrix(dir string) (IncMatrix, []string) {
	files, err := filepath.Glob(dir + "/*") // ioutil.ReadDir("./texts/")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Indexing %v documents.", len(files))

	docNames := make([]string, len(files))
	incMatrix := make(IncMatrix)

	for i, file := range files {
		docNames[i] = file

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

		for _, token := range doc.Tokens() {
			if !IsIn(punctuation, token.Text) {
				vec := incMatrix[token.Text]
				incMatrix[token.Text] = *vec.Set(uint(i))
			}
		}
	}

	return incMatrix, docNames
}

// Count - returns the number of entries in InvIndex
func (invIndex InvIndex) Count() int { return len(invIndex) }

// Find - searches for the given token in an inverse index
func (invIndex InvIndex) Find(token string) []string {
	index := invIndex[token]
	entries := make([]string, index.Cardinality())
	i := 0
	for doc := range index.Iter() {
		entries[i] = doc.(string)
		i++
	}

	return entries
}

// Find - searches for the given token in an inverse index
func (incMatrix IncMatrix) Find(token string, docNames []string) []string {
	vec := incMatrix[token]
	entries := make([]string, vec.Count())
	i := 0
	for bit, set := vec.NextSet(0); set; bit, set = vec.NextSet(bit + 1) {
		entries[i] = docNames[bit]
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

// Vals - converts token to incidence bitset
func (incMatrix IncMatrix) Vals(t string) (result bitset.BitSet) {
	if set, ok := incMatrix[t]; ok {
		return set
	}

	return
}

// And - logical AND
func And(t1 bitset.BitSet, t2 bitset.BitSet) bitset.BitSet {
	return *t1.Intersection(&t2)
}

// Or - logical OR
func Or(t1 bitset.BitSet, t2 bitset.BitSet) bitset.BitSet {
	return *t1.Union(&t2)
}

// Not - logical NOT
func Not(t bitset.BitSet) bitset.BitSet {
	return *t.Complement()
}

// IncToDocuments - translates incidence bitset to document names
func IncToDocuments(vec bitset.BitSet, docNames []string) []string {
	entries := make([]string, vec.Count())
	i := 0
	for bit, set := vec.NextSet(0); set; bit, set = vec.NextSet(bit + 1) {
		entries[i] = docNames[bit]
		i++
	}

	return entries
}

// IndToDocuments - translates index to document names
func IndToDocuments(ind mapset.Set) []string {
	entries := make([]string, ind.Cardinality())
	i := 0
	for doc := range ind.Iter() {
		entries[i] = doc.(string)
		i++
	}

	return entries
}

// Vals - converts token to inverted index
func (invIndex InvIndex) Vals(t string) (result mapset.Set) {
	if set, ok := invIndex[t]; ok {
		return set
	}

	return
}

// And2 - logical AND
func And2(t1 mapset.Set, t2 mapset.Set) mapset.Set {
	return t1.Intersect(t2)
}

// Or2 - logical OR
func Or2(t1 mapset.Set, t2 mapset.Set) mapset.Set {
	return t1.Union(t2)
}

// Not2 - logical NOT
func Not2(t mapset.Set, full mapset.Set) mapset.Set {
	return full.Difference(t)
}

func main() {
	// invIndexFile := "inv-index.gob"
	// CreateInvIndex("./texts").Save(invIndexFile)
	// invIndex := Load(invIndexFile)

	invIndex, fullSet := CreateInvIndex("./texts")
	incMatrix, docNames := CreateIncMatrix("./texts")

	fmt.Println("Unique tokens count:", invIndex.Count())

	// ==================================

	token := "input"

	entries := invIndex.Find(token)
	fmt.Println(token, ":", strings.Join(entries, ", "))

	token = "users"

	entries = invIndex.Find(token)
	fmt.Println(token, ":", strings.Join(entries, ", "))

	token = "был"

	entries = invIndex.Find(token)
	fmt.Println(token, ":", strings.Join(entries, ", "))

	// Incidence Matrix
	inc := Or(incMatrix.Vals("был"), Not(And(incMatrix.Vals("input"), incMatrix.Vals("users"))))
	entries = IncToDocuments(inc, docNames)
	fmt.Println("был OR NOT (input AND users):", strings.Join(entries, ", "))

	// Inverted Index
	ind := Or2(invIndex.Vals("был"), Not2(And2(invIndex.Vals("input"), invIndex.Vals("users")), fullSet))
	entries = IndToDocuments(ind)
	fmt.Println("был OR NOT (input AND users):", strings.Join(entries, ", "))
}
