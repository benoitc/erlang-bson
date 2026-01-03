# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-01-03

### Added

- `bson_iter` module for zero-copy BSON binary traversal
  - `new/1` - Create iterator from BSON binary with validation
  - `next/1` - Iterate elements without decoding values
  - `peek/2` - Find key at top level without iteration state
  - `find_path/2` - Navigate nested documents via path
  - `decode_value/2` - Decode value refs to Erlang terms

- `bson_codec` module for map encode/decode
  - `encode_map/1` - Encode Erlang map to BSON binary
  - `decode_map/1` - Decode BSON binary to Erlang map

- `bson_types.hrl` header with BSON type constants

- Supported BSON types:
  - double (float)
  - string (binary)
  - document (map)
  - array (list)
  - binary with subtypes
  - objectid
  - boolean
  - datetime
  - null
  - int32
  - int64
  - timestamp
  - decimal128
  - regex
  - javascript
  - minkey/maxkey

- Memory safety via `binary:copy/1` to prevent retention of large source binaries

- Comprehensive test suite (101 tests)
