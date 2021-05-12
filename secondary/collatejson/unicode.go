//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// +build ignore

package collatejson

//import "code.google.com/p/go.text/collate"
//import "code.google.com/p/go.text/collate/colltab"
//import "code.google.com/p/go.text/language"
//import "code.google.com/p/go.text/unicode/norm"

// UnicodeCollationPriority sets collate.Collator properties for unicode
// collation.
func (codec *Codec) UnicodeCollationPriority(
	strength colltab.Level, alternate collate.AlternateHandling,
	backwards, hiraganaQ, caseLevel, numeric bool) {
	codec.strength = strength
	codec.alternate = alternate
	codec.backwards = backwards
	codec.hiraganaQ = hiraganaQ
	codec.caseLevel = caseLevel
	codec.numeric = numeric
}

// SetLanguage uses language tag while doing unicode collation.
func (codec *Codec) SetLanguage(l language.Tag) {
	codec.language = l
}

// EncodeUnicodeString encodes string in utf8 encoding to binary sequence based
// on UTF8, NFKD or go.text/collate algorithms.
func (codec *Codec) EncodeUnicodeString(value string) (code []byte) {
	bs := []byte(value)
	if codec.utf8 {
		code = []byte(bs)
	} else if codec.nfkd {
		code = norm.NFKD.Bytes([]byte(bs)) // canonical decomposed
	} else {
		// TODO: Try to understand the performance implication of collate.Buffer
		// object
		buf := &collate.Buffer{}
		c := collate.New(codec.language)
		c.Strength = codec.strength
		c.Alternate = codec.alternate
		c.Backwards = codec.backwards
		c.HiraganaQuaternary = codec.hiraganaQ
		c.CaseLevel = codec.caseLevel
		c.Numeric = codec.numeric
		code = c.Key(buf, []byte(bs))
	}
	return code
}

// SortbyNFKD will enable an alternate collation using NFKD unicode standard.
func (codec *Codec) SortbyNFKD(what bool) {
	codec.nfkd = what
}

// SortbyUTF8 will do plain binary comparision for strings.
func (codec *Codec) SortbyUTF8(what bool) {
	codec.utf8 = what
}
