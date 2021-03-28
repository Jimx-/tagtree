package tagtree

import (
	"github.com/prometheus/prometheus/tsdb/labels"
)

func fromTagTreeLabels(lbls Labels) labels.Labels {
	lset := labels.Labels{}

	for i := 0; i < int(lbls.Size()); i++ {
		lset = append(lset, labels.Label{Name: lbls.Get(i).GetName(), Value: lbls.Get(i).GetValue()})
	}

	return lset
}

func toTagTreeLabels(labels labels.Labels) Labels {
	lset := NewLabels()

	for _, lbl := range labels {
		lset.Add(NewLabel(lbl.Name, lbl.Value))
	}

	return lset
}

func GetSeriesLabels(server IndexServerWrapper, tsid uint64) labels.Labels {
	lbls := NewLabels()
	server.SeriesLabels(tsid, lbls)
	return fromTagTreeLabels(lbls)
}

func AddSeries(server IndexServerWrapper, timestamp int64, lbls labels.Labels) SeriesRefBoolPair {
	return server.AddSeries(timestamp, toTagTreeLabels(lbls))

}

func toTagTreeLabelMatcher(lm labels.Matcher) LabelMatcher {
	em, ok := lm.(*labels.EqualMatcher)
	if ok {
		return NewLabelMatcher(MatchOp_EQL, lm.Name(), em.Value())
	}

	rm, ok := lm.(*labels.RegexpMatcher)
	if ok {
		return NewLabelMatcher(MatchOp_EQL_REGEX, lm.Name(), rm.Value())
	}

	nm, ok := lm.(*labels.NotMatcher)
	if ok {
		inner := toTagTreeLabelMatcher(nm.Matcher)

		if inner.GetOp() == MatchOp_EQL {
			inner.SetOp(MatchOp_NEQ)
		} else if inner.GetOp() == MatchOp_NEQ {
			inner.SetOp(MatchOp_EQL)
		} else if inner.GetOp() == MatchOp_EQL_REGEX {
			inner.SetOp(MatchOp_NEQ_REGEX)
		} else if inner.GetOp() == MatchOp_NEQ_REGEX {
			inner.SetOp(MatchOp_EQL_REGEX)
		}

		return inner
	}

	return nil
}

func ResolveLabelMatchers(server IndexServerWrapper, mint, maxt int64, matchers ...labels.Matcher) []labels.Tsid {
	lms := NewVecLabelMatcher()

	for _, lm := range matchers {
		lms.Add(toTagTreeLabelMatcher(lm))
	}

	bitmap := NewVecTSID()

	server.ResolveLabelMatchers(lms, mint, maxt, bitmap)

	results := make([]labels.Tsid, bitmap.Size())
	for i := 0; i < int(bitmap.Size()); i++ {
		results[i] = labels.Tsid(bitmap.Get(i))
	}

	return results
}
