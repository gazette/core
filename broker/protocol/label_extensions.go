package protocol

import (
	"bytes"
	"regexp"
	"sort"
	"strings"

	"go.gazette.dev/core/labels"
)

// Validate returns an error if the Label is not well-formed.
func (m Label) Validate() error {
	return m.validate(false)
}
func (m Label) validate(allowPrefix bool) error {
	if err := ValidateToken(m.Name, TokenSymbols, minLabelLen, maxLabelLen); err != nil {
		return ExtendContext(err, "Name")
	} else if err = ValidateToken(m.Value, pathSymbols, 0, maxLabelValueLen); err != nil {
		return ExtendContext(err, "Value")
	} else if m.Prefix && !allowPrefix {
		return NewValidationError("Prefix may not be set outside of a LabelSelector")
	}
	return nil
}

// MustLabelSet is a convenience for constructing LabelSets from a sequence of
// Name, Value arguments. The result LabelSet must Validate or MustLabelSet panics.
func MustLabelSet(nv ...string) (set LabelSet) {
	if len(nv)%2 != 0 {
		panic("expected an even number of Name/Value arguments")
	}
	for i := 0; i != len(nv); i += 2 {
		set.AddValue(nv[i], nv[i+1])
	}
	if err := set.validate(true); err != nil {
		panic(err.Error())
	}
	return
}

// Validate returns an error if the LabelSet is not well-formed.
func (m LabelSet) Validate() error {
	return m.validate(false)
}
func (m LabelSet) validate(allowPrefix bool) error {
	for i := range m.Labels {
		if err := m.Labels[i].validate(allowPrefix); err != nil {
			return ExtendContext(err, "Labels[%d]", i)
		} else if i == 0 {
			continue
		}

		if m.Labels[i-1].Name < m.Labels[i].Name {
			// Pass.
		} else if m.Labels[i-1].Name == m.Labels[i].Name {
			if m.Labels[i-1].Value >= m.Labels[i].Value {
				return NewValidationError("Labels not in unique, sorted order (index %d; label %s value %s <= %s)",
					i, m.Labels[i].Name, m.Labels[i].Value, m.Labels[i-1].Value)
			} else if m.Labels[i-1].Value == "" {
				return NewValidationError("Label has empty & non-empty values (index %d; label %s value %s)",
					i, m.Labels[i].Name, m.Labels[i].Value)
			}
		} else {
			return NewValidationError("Labels not in unique, sorted order (index %d; label name %s <= %s)",
				i, m.Labels[i].Name, m.Labels[i-1].Name)
		}
	}
	return nil
}

// Assign the labels of the |other| set to this LabelSet. If |other| is nil,
// this LabelSet is emptied.
func (m *LabelSet) Assign(other *LabelSet) {
	if m.Labels = m.Labels[:0]; other != nil {
		m.Labels = append(m.Labels, other.Labels...)
	}
}

// ValuesOf returns the values of Label |name|, or nil if it doesn't exist in the LabelSet.
func (m LabelSet) ValuesOf(name string) (values []string) {
	var ind = sort.Search(len(m.Labels), func(i int) bool { return m.Labels[i].Name >= name })

	for ind != len(m.Labels) && m.Labels[ind].Name == name {
		values = append(values, m.Labels[ind].Value)
		ind++
	}
	return
}

// ValueOf returns the first value of Label |name|, or "" if it doesn't exist in the LabelSet.
func (m LabelSet) ValueOf(name string) string {
	var ind = sort.Search(len(m.Labels), func(i int) bool { return m.Labels[i].Name >= name })

	if ind != len(m.Labels) && m.Labels[ind].Name == name {
		return m.Labels[ind].Value
	}
	return ""
}

// SetValue adds Label |name| with |value|, replacing any existing Labels |name|.
// If |name| has the special suffix ":prefix", the Label is marked as a prefix
// match. It's only valid to use ":prefix" within the context of a LabelSelector.
func (m *LabelSet) SetValue(name, value string) {
	var prefix = strings.HasSuffix(name, ":prefix")
	name = strings.TrimSuffix(name, ":prefix")

	var begin = sort.Search(len(m.Labels), func(i int) bool { return m.Labels[i].Name >= name })

	var end = begin
	for end != len(m.Labels) && m.Labels[end].Name == name {
		end++
	}
	if begin == end {
		m.Labels = append(m.Labels, Label{})
		copy(m.Labels[begin+1:], m.Labels[begin:]) // Insert at |begin|.
		end = begin + 1
	}

	m.Labels[begin] = Label{Name: name, Value: value, Prefix: prefix}
	m.Labels = append(m.Labels[:begin+1], m.Labels[end:]...) // Cut |begin+1, end).
}

// AddValue adds Label |name| with |value|, retaining any existing Labels |name|.
// If |name| has the special suffix ":prefix", the Label is marked as a prefix
// match. It's only valid to use ":prefix" within the context of a LabelSelector.
func (m *LabelSet) AddValue(name, value string) {
	var prefix = strings.HasSuffix(name, ":prefix")
	name = strings.TrimSuffix(name, ":prefix")

	var ind = sort.Search(len(m.Labels), func(i int) bool {
		if m.Labels[i].Name != name {
			return m.Labels[i].Name > name
		} else {
			return m.Labels[i].Value >= value
		}
	})
	var label = Label{Name: name, Value: value, Prefix: prefix}

	if ind != len(m.Labels) && m.Labels[ind] == label {
		// No-op.
	} else {
		m.Labels = append(m.Labels, Label{})
		copy(m.Labels[ind+1:], m.Labels[ind:])
		m.Labels[ind] = label
	}
}

// Remove all instances of Labels |name|.
func (m *LabelSet) Remove(name string) {
	var begin = sort.Search(len(m.Labels), func(i int) bool { return m.Labels[i].Name >= name })

	var end = begin
	for end != len(m.Labels) && m.Labels[end].Name == name {
		end++
	}
	m.Labels = append(m.Labels[:begin], m.Labels[end:]...) // Cut |begin, end).
}

// ValidateSingleValueLabels compares the LabelSet to labels.SingleValueLabels,
// and returns an error if any labels have multiple values.
func ValidateSingleValueLabels(m LabelSet) error {
	for i := range m.Labels {
		if i == 0 || m.Labels[i-1].Name != m.Labels[i].Name {
			continue
		}
		if _, ok := labels.SingleValueLabels[m.Labels[i].Name]; ok {
			return NewValidationError("expected single-value Label has multiple values (index %d; label %s value %s)",
				i, m.Labels[i].Name, m.Labels[i].Value)
		}
	}
	return nil
}

// UnionLabelSets returns the LabelSet having all labels present in either |lhs|
// or |rhs|. Where both |lhs| and |rhs| have values for a label, those of |lhs|
// are preferred.
func UnionLabelSets(lhs, rhs, out LabelSet) LabelSet {
	out.Labels = out.Labels[:0]

	var it = labelJoin{
		setL: lhs.Labels,
		setR: rhs.Labels,
		lenL: len(lhs.Labels),
		lenR: len(rhs.Labels),
	}
	for cur, ok := it.next(); ok; cur, ok = it.next() {
		if cur.lBeg != cur.lEnd { // Prefer |lhs| label values.
			out.Labels = append(out.Labels, lhs.Labels[cur.lBeg:cur.lEnd]...)
		} else {
			out.Labels = append(out.Labels, rhs.Labels[cur.rBeg:cur.rEnd]...)
		}
	}
	return out
}

// IntersectLabelSets returns the LabelSet having all labels present in both
// |lhs| and |rhs| with matched values.
func IntersectLabelSets(lhs, rhs, out LabelSet) LabelSet {
	out.Labels = out.Labels[:0]

	var it = labelJoin{
		setL: lhs.Labels,
		setR: rhs.Labels,
		lenL: len(lhs.Labels),
		lenR: len(rhs.Labels),
	}
	for cur, ok := it.next(); ok; cur, ok = it.next() {
		if labelValuesEqual(it, cur) {
			out.Labels = append(out.Labels, lhs.Labels[cur.lBeg:cur.lEnd]...)
		}
	}
	return out
}

// SubtractLabelSets returns the LabelSet having labels in |lhs| which are not
// present in |rhs| with matched values.
func SubtractLabelSet(lhs, rhs, out LabelSet) LabelSet {
	out.Labels = out.Labels[:0]

	var it = labelJoin{
		setL: lhs.Labels,
		setR: rhs.Labels,
		lenL: len(lhs.Labels),
		lenR: len(rhs.Labels),
	}
	for cur, ok := it.next(); ok; cur, ok = it.next() {
		if !labelValuesEqual(it, cur) {
			out.Labels = append(out.Labels, lhs.Labels[cur.lBeg:cur.lEnd]...)
		}
	}
	return out
}

// Validate returns an error if the LabelSelector is not well-formed.
func (m LabelSelector) Validate() error {
	if err := m.Include.validate(true); err != nil {
		return ExtendContext(err, "Include")
	} else if err := m.Exclude.validate(true); err != nil {
		return ExtendContext(err, "Exclude")
	}
	return nil
}

// Matches returns whether the LabelSet is matched by the LabelSelector.
func (m LabelSelector) Matches(s LabelSet) bool {
	if matchSelector(m.Exclude.Labels, s.Labels, false) {
		return false // At least one excluded label is matched.
	} else if !matchSelector(m.Include.Labels, s.Labels, true) {
		return false // Not every included label is matched.
	}
	return true
}

// String returns a canonical string representation of the LabelSelector.
func (s LabelSelector) String() string {
	var w = bytes.NewBuffer(nil)

	var f = func(input []Label, exc bool) {
		var l []Label

		// Attach literal ":prefix" suffixes to names so that prefixes are
		// distinguished during the subsequent self-join over `l`.
		for _, ll := range input {
			if ll.Prefix {
				ll.Name = ll.Name + ":prefix"
			}
			l = append(l, ll)
		}
		// We may have changed Name ordering. Re-index.
		sort.Slice(l, func(i, j int) bool { return l[i].Name < l[j].Name })

		var it = labelJoin{setL: l, setR: l, lenL: len(l), lenR: len(l)}
		for cur, ok := it.next(); ok; cur, ok = it.next() {
			if cur.lBeg+1 == cur.lEnd && l[cur.lBeg].Value == "" {
				if exc {
					w.WriteByte('!')
				}
				w.WriteString(l[cur.lBeg].Name)
			} else if cur.lBeg+1 == cur.lEnd {
				w.WriteString(l[cur.lBeg].Name)

				if exc {
					w.WriteByte('!')
				}
				w.WriteByte('=')

				w.WriteString(l[cur.lBeg].Value)
			} else {
				w.WriteString(l[cur.lBeg].Name)

				if exc {
					w.WriteString(" notin (")
				} else {
					w.WriteString(" in (")
				}
				w.WriteString(l[cur.lBeg].Value)
				for ; cur.lBeg+1 != cur.lEnd; cur.lBeg++ {
					w.WriteByte(',')
					w.WriteString(l[cur.lBeg+1].Value)
				}
				w.WriteByte(')')
			}
			if cur.lEnd != len(l) {
				w.WriteByte(',')
			}
		}
	}
	f(s.Include.Labels, false)
	if w.Len() != 0 && len(s.Exclude.Labels) != 0 {
		w.WriteByte(',')
	}
	f(s.Exclude.Labels, true)

	return w.String()
}

func matchSelector(sel, set []Label, reqAll bool) bool {
	var it = labelJoin{
		setL: sel,
		setR: set,
		lenL: len(sel),
		lenR: len(set),
	}
	for cur, ok := it.next(); ok; cur, ok = it.next() {
		if cur.lBeg == cur.lEnd {
			continue // This is a |set| label which is not in |sel|.
		}
		// Determine if at least one label value of |sel| is present in |set|.
		var matched bool

		for a, b := sel[cur.lBeg:cur.lEnd], set[cur.rBeg:cur.rEnd]; !matched && len(a) != 0 && len(b) != 0; {
			if a[0].Value == "" ||
				(!a[0].Prefix && a[0].Value == b[0].Value) ||
				(a[0].Prefix && strings.HasPrefix(b[0].Value, a[0].Value)) {
				matched = true // Selector value "" implicitly matches any value of the label.
			} else if a[0].Value < b[0].Value {
				a = a[1:]
			} else {
				b = b[1:]
			}
		}

		if !reqAll && matched {
			return true
		} else if reqAll && !matched {
			return false
		}
	}
	return reqAll
}

// labelJoin performs a full outer join of two sorted []Label sets.
type labelJoin struct {
	setL, setR []Label
	lenL, lenR int
	labelJoinCursor
}

type labelJoinCursor struct {
	lBeg, lEnd, rBeg, rEnd int
}

func (j *labelJoin) next() (cur labelJoinCursor, ok bool) {
	var c int

	if j.lBeg == j.lenL && j.rBeg == j.lenR {
		cur = j.labelJoinCursor
		return // Both sequences complete.
	} else if j.lBeg == j.lenL {
		c = +1 // LHS sequence complete. Step RHS.
	} else if j.rBeg == j.lenR {
		c = -1 // RHS sequence complete. Step LHS.
	} else if j.setL[j.lBeg].Name == j.setR[j.rBeg].Name {
		c = 0 // Step both.
	} else if j.setL[j.lBeg].Name < j.setR[j.rBeg].Name {
		c = -1 // Step LHS.
	} else {
		c = +1 // Step RHS.
	}

	for j.lEnd != j.lenL && c <= 0 && j.setL[j.lEnd].Name == j.setL[j.lBeg].Name {
		j.lEnd++
	}
	for j.rEnd != j.lenR && c >= 0 && j.setR[j.rEnd].Name == j.setR[j.rBeg].Name {
		j.rEnd++
	}

	cur, ok = j.labelJoinCursor, true
	j.lBeg, j.rBeg = j.lEnd, j.rEnd

	return
}

// labelValuesEqual returns true if the values of the labels indicated by the
// labelJoinCursor of the labelJoin are exactly matched.
func labelValuesEqual(it labelJoin, cur labelJoinCursor) bool {
	if n := cur.lEnd - cur.lBeg; n != (cur.rEnd - cur.rBeg) {
		return false
	} else {
		for i := 0; i != n; i++ {
			if it.setL[cur.lBeg+i].Value != it.setR[cur.rBeg+i].Value {
				return false
			}
		}
	}
	return true
}

// ParseLabelSelector parses a LabelSelector string. Selector strings are
// composed of a comma-separate list of selector expressions. Allowed
// expression types are equality, in-equality, set membership, set exclusion,
// existence, and non-existence. Eg:
//
//   - "foo = bar" requires that label "foo" be present with value "bar"
//   - "foo != bar" requires that label "foo" not be present with value "bar"
//   - "foo" requires that label "foo" be present (with any value).
//   - "!foo" requires that label "foo" not be present.
//   - "foo in (bar,baz)" requires that "foo" be present with either "bar" or "baz".
//   - "foo notin (bar,baz)" requires that "foo", if present, not have value "bar" or "baz".
//
// A label name within a selector can have a "my-label:prefix" suffix which
// tests whether the selector value is a prefix of the named label value,
// instead of using exact equality.
// For example, "foo:prefix in (one/two/, three/)" would match "one/two/three"
// and "three/four/five" but not "one/one/" or "four/five/six".
//
// Additional examples of composite expressions:
//   - "topic in (topic/one, topic/two), name:prefix=/my/journal/prefix"
//   - "env in (production, qa), tier not in (frontend,backend), partition"
//
// ParseLabelSelector is invariant to _reasonable_ spacing: eg, "not in" and
// "notin" may be used interchangeably, as may "==" and "=", with or without
// single spacing on either side.
func ParseLabelSelector(s string) (LabelSelector, error) {
	var out LabelSelector

	for len(s) != 0 {
		var m []string
		if m = reSelectorEqual.FindStringSubmatch(s); m != nil {
			out.Include.AddValue(m[1], m[2])
		} else if m = reSelectorNotEqual.FindStringSubmatch(s); m != nil {
			out.Exclude.AddValue(m[1], m[2])
		} else if m = reSelectorSetIn.FindStringSubmatch(s); m != nil {
			if err := parseSetParts(&out.Include, m[1], m[2]); err != nil {
				return LabelSelector{}, ExtendContext(err, "parsing %q", s)
			}
		} else if m = reSelectorSetNotIn.FindStringSubmatch(s); m != nil {
			if err := parseSetParts(&out.Exclude, m[1], m[2]); err != nil {
				return LabelSelector{}, ExtendContext(err, "parsing %q", s)
			}
		} else if m = reSelectorSetExists.FindStringSubmatch(s); m != nil {
			out.Include.AddValue(m[1], "")
		} else if m = reSelectorSetNotExists.FindStringSubmatch(s); m != nil {
			out.Exclude.AddValue(m[1], "")
		} else {
			return LabelSelector{}, NewValidationError("could not match %q to a label selector expression", s)
		}
		s = s[len(m[0]):]
	}

	return out, out.Validate()
}

// MustLabelSelector is a convenience for constructing a parsed LabelSelector.
// The LabelSelector must parse and Validate or MustLabelSelector panics.
func MustLabelSelector(selector string) LabelSelector {
	if out, err := ParseLabelSelector(selector); err != nil {
		panic(err.Error())
	} else {
		return out
	}
}

func parseSetParts(set *LabelSet, name, s string) error {
	for len(s) != 0 {
		var m []string

		if m = reSelectorSetValue.FindStringSubmatch(s); m != nil {
			set.AddValue(name, m[1])
		} else {
			return NewValidationError("could not match %q to a label selector set expression", s)
		}
		s = s[len(m[0]):]
	}
	return nil
}

var (
	reToken         = ` ?([\pL\pN\` + regexp.QuoteMeta(TokenSymbols) + `]{2,}(?:\:prefix)?)`
	rePath          = ` ?([\pL\pN\` + regexp.QuoteMeta(pathSymbols) + `]{0,})`
	reCommaOrEnd    = ` ?(?:,|$)`
	reParenthetical = ` ?\(([^)]+)\)`

	reSelectorEqual    = regexp.MustCompile(`^` + reToken + ` ?=?=` + rePath + reCommaOrEnd)
	reSelectorNotEqual = regexp.MustCompile(`^` + reToken + ` ?!=` + rePath + reCommaOrEnd)

	reSelectorSetIn        = regexp.MustCompile(`^` + reToken + ` in` + reParenthetical + reCommaOrEnd)
	reSelectorSetNotIn     = regexp.MustCompile(`^` + reToken + ` not ?in` + reParenthetical + reCommaOrEnd)
	reSelectorSetExists    = regexp.MustCompile(`^` + reToken + reCommaOrEnd)
	reSelectorSetNotExists = regexp.MustCompile(`^ ?!` + reToken + reCommaOrEnd)
	reSelectorSetValue     = regexp.MustCompile(`^` + rePath + reCommaOrEnd)
)

const (
	minLabelLen, maxLabelLen = 2, 64
	maxLabelValueLen         = 1024
)
