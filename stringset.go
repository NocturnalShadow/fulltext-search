package main

// StringSet - a set of strings represented by a hash-map
type StringSet map[string]struct{}

// NewStringSet - StringSet constructur
func NewStringSet() StringSet { return make(StringSet) }

// Add - adds a string to StringSet
func (set StringSet) Add(v string) { set[v] = struct{}{} }

// Remove - removes a string from StringSet
func (set StringSet) Remove(v string) { delete(set, v) }

// Contains - checks if StringSet contains a given string
func (set StringSet) Contains(v string) bool { _, ok := set[v]; return ok }

// Count - returns the number of entries in StringSet
func (set StringSet) Count() int { return len(set) }
