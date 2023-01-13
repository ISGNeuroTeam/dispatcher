
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.2] - 2023-01-13
### Added
- A scheme merging option (mergeSchema) to readFile.

## [1.1.1] - 2023-01-12
### Fixed
- Test writeFile.
- Changelog.

## [1.1.0] - 2023-01-12
### Changed
- Local resolver to Production (network) resolver in build.sbt. No more need in publishLocal.
### Added
- Arg "header" to readFile and writeFile commands.
- Default value "parquet" to "format" arg in readFile and writeFile commands.
- Arg "mode"  ('overwrite', 'append', 'ignore', 'error', 'errorifexists') to writeFile command.
- Arg "numPartitions" to writeFile command.
- Arg "partitionBy" to writeFile command.
- Aliases "put" and "get" to writeFile and readFile commands.

## [1.0.3] - 2022-01-31
### Changed
- plugin.conf.example.
- README.md.

## [1.0.2] - 2021-09-13
### Added
- ReleaseNotes.md.
### Changed
- Dispatcher-sdk dependency updated to 1.2.0.

## [1.0.1] - 2020-06-04
### Changed
- Update to new SDK. 

## [1.0.0] - 2020-06-04 
### Added
- This CHANGELOG.md.
