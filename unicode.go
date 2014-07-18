//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not
//  use this file except in compliance with the License. You may obtain a copy
//  of the License at http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//  License for the specific language governing permissions and limitations
//  under the License.

// +build ignore

package collatejson

import (
//"code.google.com/p/go.text/collate"
//"code.google.com/p/go.text/collate/colltab"
//"code.google.com/p/go.text/language"
//"code.google.com/p/go.text/unicode/norm"
)

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
