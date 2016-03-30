// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// Bulk request to update a document in Elasticsearch.
//
// See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
// for details.
type BulkUpdateRequest struct {
	BulkableRequest
	index string
	typ   string
	id    string

	routing         string
	parent          string
	script          *Script
	version         int64  // default is MATCH_ANY
	versionType     string // default is "internal"
	retryOnConflict *int
	refresh         *bool
	upsert          interface{}
	docAsUpsert     *bool
	doc             interface{}
	ttl             int64
	timestamp       string

	source []string
}

// NewBulkUpdateRequest returns a new BulkUpdateRequest.
func NewBulkUpdateRequest() *BulkUpdateRequest {
	return &BulkUpdateRequest{}
}

// Index specifies the Elasticsearch index to use for this update request.
// If unspecified, the index set on the BulkService will be used.
func (r *BulkUpdateRequest) Index(index string) *BulkUpdateRequest {
	r.index = index
	r.source = nil
	return r
}

// Type specifies the Elasticsearch type to use for this update request.
// If unspecified, the type set on the BulkService will be used.
func (r *BulkUpdateRequest) Type(typ string) *BulkUpdateRequest {
	r.typ = typ
	r.source = nil
	return r
}

// Id specifies the identifier of the document to update.
func (r *BulkUpdateRequest) Id(id string) *BulkUpdateRequest {
	r.id = id
	r.source = nil
	return r
}

// Routing specifies a routing value for the request.
func (r *BulkUpdateRequest) Routing(routing string) *BulkUpdateRequest {
	r.routing = routing
	r.source = nil
	return r
}

// Parent specifies the identifier of the parent document (if available).
func (r *BulkUpdateRequest) Parent(parent string) *BulkUpdateRequest {
	r.parent = parent
	r.source = nil
	return r
}

// Script specifies an update script.
// See https://www.elastic.co/guide/en/elasticsearch/reference/2.x/docs-bulk.html#bulk-update
// and https://www.elastic.co/guide/en/elasticsearch/reference/2.x/modules-scripting.html
// for details.
func (r *BulkUpdateRequest) Script(script *Script) *BulkUpdateRequest {
	r.script = script
	r.source = nil
	return r
}

// RetryOnConflict specifies how often to retry in case of a version conflict.
func (r *BulkUpdateRequest) RetryOnConflict(retryOnConflict int) *BulkUpdateRequest {
	r.retryOnConflict = &retryOnConflict
	r.source = nil
	return r
}

// Version indicates the version of the document as part of an optimistic
// concurrency model.
func (r *BulkUpdateRequest) Version(version int64) *BulkUpdateRequest {
	r.version = version
	r.source = nil
	return r
}

// VersionType can be "internal" (default), "external", "external_gte",
// "external_gt", or "force".
func (r *BulkUpdateRequest) VersionType(versionType string) *BulkUpdateRequest {
	r.versionType = versionType
	r.source = nil
	return r
}

// Refresh indicates whether to update the shards immediately after
// the request has been processed. Updated documents will appear
// in search immediately at the cost of slower bulk performance.
func (r *BulkUpdateRequest) Refresh(refresh bool) *BulkUpdateRequest {
	r.refresh = &refresh
	r.source = nil
	return r
}

// Doc specifies the updated document.
func (r *BulkUpdateRequest) Doc(doc interface{}) *BulkUpdateRequest {
	r.doc = doc
	r.source = nil
	return r
}

// DocAsUpsert indicates whether the contents of Doc should be used as
// the Upsert value.
//
// See https://www.elastic.co/guide/en/elasticsearch/reference/2.x/docs-update.html#_literal_doc_as_upsert_literal
// for details.
func (r *BulkUpdateRequest) DocAsUpsert(docAsUpsert bool) *BulkUpdateRequest {
	r.docAsUpsert = &docAsUpsert
	r.source = nil
	return r
}

// Upsert specifies the document to use for upserts. It will be used for
// create if the original document does not exist.
func (r *BulkUpdateRequest) Upsert(doc interface{}) *BulkUpdateRequest {
	r.upsert = doc
	r.source = nil
	return r
}

// Ttl specifies the time to live, and optional expiry time.
// This is deprecated as of 2.0.0-beta2.
func (r *BulkUpdateRequest) Ttl(ttl int64) *BulkUpdateRequest {
	r.ttl = ttl
	r.source = nil
	return r
}

// Timestamp specifies a timestamp for the document.
// This is deprecated as of 2.0.0-beta2.
func (r *BulkUpdateRequest) Timestamp(timestamp string) *BulkUpdateRequest {
	r.timestamp = timestamp
	r.source = nil
	return r
}

// String returns the on-wire representation of the update request,
// concatenated as a single string.
func (r *BulkUpdateRequest) String() string {
	lines, err := r.Source()
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return strings.Join(lines, "\n")
}

func (r *BulkUpdateRequest) getSourceAsString(data interface{}) (string, error) {
	switch t := data.(type) {
	default:
		body, err := json.Marshal(data)
		if err != nil {
			return "", err
		}
		return string(body), nil
	case json.RawMessage:
		return string(t), nil
	case *json.RawMessage:
		return string(*t), nil
	case string:
		return t, nil
	case *string:
		return *t, nil
	}
}

// Source returns the on-wire representation of the update request,
// split into an action-and-meta-data line and an (optional) source line.
// See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
// for details.
func (r BulkUpdateRequest) Source() ([]string, error) {
	// { "update" : { "_index" : "test", "_type" : "type1", "_id" : "1", ... } }
	// { "doc" : { "field1" : "value1", ... } }
	// or
	// { "update" : { "_index" : "test", "_type" : "type1", "_id" : "1", ... } }
	// { "script" : { ... } }

	if r.source != nil {
		return r.source, nil
	}

	lines := make([]string, 2)

	// We build the JSON via a buffer here to save time in JSON serialization.
	// This is one of the hot paths for bulk indexing.

	// "update" ...
	var comma bool
	var buf bytes.Buffer
	var add = func(k, v string) {
		if comma {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf(`%q:%s`, k, v))
		comma = true
	}
	// Keep in alphabetical order to emulate behavior of JSON serializer and tests still pass
	buf.WriteString("{")
	if r.id != "" {
		add("_id", fmt.Sprintf("%q", r.id))
	}
	if r.index != "" {
		add("_index", fmt.Sprintf("%q", r.index))
	}
	if r.parent != "" {
		add("_parent", fmt.Sprintf("%q", r.parent))
	}
	if r.retryOnConflict != nil {
		add("_retry_on_conflict", fmt.Sprintf("%d", *r.retryOnConflict))
	}
	if r.routing != "" {
		add("_routing", fmt.Sprintf("%q", r.routing))
	}
	if r.timestamp != "" {
		add("_timestamp", fmt.Sprintf("%q", r.timestamp))
	}
	if r.ttl > 0 {
		add("_ttl", fmt.Sprintf("%d", r.ttl))
	}
	if r.typ != "" {
		add("_type", fmt.Sprintf("%q", r.typ))
	}
	if r.version > 0 {
		add("_version", fmt.Sprintf("%d", r.version))
	}
	if r.versionType != "" {
		add("_version_type", fmt.Sprintf("%q", r.versionType))
	}
	if r.refresh != nil {
		if *r.refresh {
			add("refresh", "true")
		} else {
			add("refresh", "false")
		}
	}
	buf.WriteString("}")
	lines[0] = fmt.Sprintf(`{"update":%s}`, buf.String())

	// 2nd line: {"doc" : { ... }} or {"script": {...}}
	source := make(map[string]interface{})
	if r.docAsUpsert != nil {
		source["doc_as_upsert"] = *r.docAsUpsert
	}
	if r.upsert != nil {
		source["upsert"] = r.upsert
	}
	if r.doc != nil {
		// {"doc":{...}}
		source["doc"] = r.doc
	} else if r.script != nil {
		// {"script":...}
		src, err := r.script.Source()
		if err != nil {
			return nil, err
		}
		source["script"] = src
	}
	var err error
	lines[1], err = r.getSourceAsString(source)
	if err != nil {
		return nil, err
	}

	r.source = lines
	return lines, nil
}
