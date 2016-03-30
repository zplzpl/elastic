// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"bytes"
	"fmt"
	"strings"
)

// -- Bulk delete request --

// Bulk request to remove a document from Elasticsearch.
//
// See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
// for details.
type BulkDeleteRequest struct {
	BulkableRequest
	index       string
	typ         string
	id          string
	routing     string
	refresh     *bool
	version     int64  // default is MATCH_ANY
	versionType string // default is "internal"

	source []string
}

// NewBulkDeleteRequest returns a new BulkDeleteRequest.
func NewBulkDeleteRequest() *BulkDeleteRequest {
	return &BulkDeleteRequest{}
}

// Index specifies the Elasticsearch index to use for this delete request.
// If unspecified, the index set on the BulkService will be used.
func (r *BulkDeleteRequest) Index(index string) *BulkDeleteRequest {
	r.index = index
	r.source = nil
	return r
}

// Type specifies the Elasticsearch type to use for this delete request.
// If unspecified, the type set on the BulkService will be used.
func (r *BulkDeleteRequest) Type(typ string) *BulkDeleteRequest {
	r.typ = typ
	r.source = nil
	return r
}

// Id specifies the identifier of the document to delete.
func (r *BulkDeleteRequest) Id(id string) *BulkDeleteRequest {
	r.id = id
	r.source = nil
	return r
}

// Routing specifies a routing value for the request.
func (r *BulkDeleteRequest) Routing(routing string) *BulkDeleteRequest {
	r.routing = routing
	r.source = nil
	return r
}

// Refresh indicates whether to update the shards immediately after
// the delete has been processed. Deleted documents will disappear
// in search immediately at the cost of slower bulk performance.
func (r *BulkDeleteRequest) Refresh(refresh bool) *BulkDeleteRequest {
	r.refresh = &refresh
	r.source = nil
	return r
}

// Version indicates the version to be deleted as part of an optimistic
// concurrency model.
func (r *BulkDeleteRequest) Version(version int64) *BulkDeleteRequest {
	r.version = version
	r.source = nil
	return r
}

// VersionType can be "internal" (default), "external", "external_gte",
// "external_gt", or "force".
func (r *BulkDeleteRequest) VersionType(versionType string) *BulkDeleteRequest {
	r.versionType = versionType
	r.source = nil
	return r
}

// String returns the on-wire representation of the delete request,
// concatenated as a single string.
func (r *BulkDeleteRequest) String() string {
	lines, err := r.Source()
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return strings.Join(lines, "\n")
}

// Source returns the on-wire representation of the delete request,
// split into an action-and-meta-data line and an (optional) source line.
// See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
// for details.
func (r *BulkDeleteRequest) Source() ([]string, error) {
	if r.source != nil {
		return r.source, nil
	}

	// We build the JSON via a buffer here to save time in JSON serialization.
	// This is one of the hot paths for bulk indexing.

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
	if r.routing != "" {
		add("_routing", fmt.Sprintf("%q", r.routing))
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

	r.source = []string{fmt.Sprintf(`{"delete":%s}`, buf.String())}

	return r.source, nil
}
