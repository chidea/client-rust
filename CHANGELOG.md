# Changelog

All changes in this project will be noted in this file.

## 0.8.9

### Additions

- Added enhanced multi-row decode support
- Added deref to slice for rows

### Fixes

- Fixed error propagation in `FromResponse` impls

## 0.8.8

### Fixes

- Fixed response decoder and handling issues

### Additions

- Added `FromResponse` for `Vec<Row>`
- Added `SQParam` impl for `&Vec<u8>`

## 0.8.7

> - **Field change warnings**:
>   - The `Config` struct now has one additional field. This is not a breaking change because the functionality of the library remains unchanged

### Additions

- Added support for pipelines
- Added `Response::parse` to convert a response into compatible types

## 0.8.6

Reduced allocations in `Query`.

## 0.8.5

Fixed bugs with the derive macros.

## 0.8.4

> **Yanked version**

Fixed an issue with single-item struct derives when using the `Response` macro.

## 0.8.3

Added the following implementations:
- `FromResponse` for `Row`
- `FromValue` for `Value` (this was erroneously missing)
- Added the `Value::parse` and `Value::parse_cloned` member methods
- Added `Row::into_first` and `Row::into_first_as` member methods

## 0.8.2

Support deriving queries and responses.

## 0.8.1

Fixed issues with documentation

## 0.8.0

### New features
- Completely up to date for Skyhash 2.0
- New query API interface for Skytable Octave (completely breaking!)
- No longer depends on OpenSSL

### Breaking changes
The enter query interface as changed and is incompatible with previous driver versions. Please consider reading the Skytable
Octave upgrade guide.

## 0.7.0

### New features

- Sync connection pooling
- Async connection pooling
- Added `run_query_raw` and `run_query` that lets you specify custom types:
  ```rust
  use skytable::{query, sync::Connection};
  let mut con = Connection::new("127.0.0.1", "2003").unwrap();
  let string: String = con.run_query(query!("heya")).unwrap();
  let number: usize = con.run_query(query!("dbsize")).unwrap();
  ```

### Breaking changes

- `SkyRawResult` is now `SkyResult`
- `SkyResult` is now `SkyQueryResult`
- The feature `async` is now `aio`
- `Query::add` is now `Query::append` to avoid confusion with the `core::ops::Add` trait
- `Element::Float` is now an `f32` instead of an `f64` because the Skyhash specification requires it
  (this was mistakenly an `f64` but shouldn't be a problem because no actions returned any floating
  point value)

## Version 0.6.2

- Added support for pipelined queries
- Added support for the `whereami` action
- Added support for non-null typed arrays

## Version 0.6.1

> Breaking changes!

### Fixes

- Fixed missing entity name in query generation for DDL's `create_table` function

### Breaking

- The inner type of the `entity` field in `ddl::Keymap` was changed to `String` instead of `Option<String>`. Since this
  was never a public field, it should not affect you. However, if you depend on `Debug` fmt implementations then you should
  keep this in mind

## Version 0.6.0

> Breaking changes

### Added

- Support for DDL queries
- Support for directly getting types from actions (this is required to be passed for actions
  that need them). For example:
  ```rust
  let x: u64 = con.get("my integer key").unwrap();
  let myval: Vec<String> = con.mget(["x", "y", "z"]).unwrap();
  ```
  All errors resulting from this parse are simply propagated into the `Error::ParseError`
  variant
- Support for retrieval of custom types through the use of the `FromSkyhashBytes` trait

### Breaking changes

- Errors have been unified into a single error type
- Some types have been removed to aid simplicity:
  - `types::Str`
  - `types::SimpleArray`
- The trait method `IntoSkyhashBytes::into_bytes` was renamed to `IntoSkyhashBytes::as_bytes()` to
  follow naming conventions

## Version 0.5.0

> Breaking changes

- Added support for Skyhash 1.1
- Changed `Query` type to `AnyArray` as in the latest protocol

## Version 0.4.0

> Breaking changes

- Added backward compatibility for Rust versions < 1.51 (
  people who need const-generics just add `const-gen` to their feature list)
- Added support for the `POP` query type
- `into_string` is now called `as_string` to follow naming conventions (trait `IntoSkyhashBytes`)
- New SSL features:
  - `ssl`: For sync TLS
  - `sslv`: For sync TLS (vendored)
  - `aio-ssl`: For async TLS
  - `aio-sslv`: For async TLS (vendored)

## Version 0.3.0

> Breaking changes

- Dropped support for Terrapipe 1.0
- Added support for Skyhash 1.0
- Response variants have changed according to the protocol
- Added `sync` and `async` features with `sync` as a default feature and `async` as an optional feature
- Queries constructed using `Query::arg()` now follow the builder pattern
- Queries can be constructed by taking references using `Query::push`
- `run_simple_query` now takes a reference to a `Query` instead of taking ownership of it
- Actions can now be run by importing `skytable::actions::Actions` (or `skytable::actions::AsyncActions` for the `async` API).  
  For example:
  `rust use skytable::{Connection, actions::Actions}; fn main() { let mut con = Connection::new("127.0.0.1", 2003).unwrap(); con.set("x", "100").unwrap(); assert_eq!(con.get("x").unwrap(), "100".to_owned()); } `
- `run_simple_query` (both sync and async) verify whether the query is empty or not. If it is, the function will
  panic. This is a very important check to avoid confusion as the server would return a `PacketError` which might
  create additional confusion
- `Query` objects can now be constructed with the `From` trait on appropriate types (such as single items or
  sequences)

## Version 0.2.0

> Breaking changes

- Fixed `Response` variant returning `Vec<DataGroup>` instead of just `DataGroup`
- Implemented `IntoIterator` for `DataGroup`

## Version 0.1.0

> This release has been yanked because it returned the incorrect type in the `Response` enum

Initial release
