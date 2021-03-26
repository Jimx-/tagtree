%module tagtree

%{
#include "tagtree/swig/wrapper.h"
    using namespace tagtree;
%}

%include "typemaps.i"
%include "std_string.i"
%include "std_vector.i"
%include "std_pair.i"

%include "tagtree/swig/wrapper.h"
%include "promql/labels.h"
%include "tagtree/wal/records.h"

%template(Labels) std::vector<promql::Label>;
%template(VecSeriesRef) std::vector<SeriesRef>;
%template(VecLabelMatcher) std::vector<promql::LabelMatcher>;
%template(VecTSID) std::vector<unsigned long>;

%template(StringPair) std::pair<std::string, std::string>;
%template(SeriesRefBoolPair) std::pair<tagtree::SeriesRef, bool>;
