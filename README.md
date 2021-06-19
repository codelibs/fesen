Fesen: Fess Search Engine
[![Java CI with Maven](https://github.com/codelibs/fesen/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fesen/actions/workflows/maven.yml)
------------------

## Overview

Fesen is a search engine for Fess.
This product is forked from Elasticsearch 7.10.2 and also optimized for Fess.

## Usage

### Run Fesen

```
$ unzip fesen-0.10.0.zip
$ cd fesen-0.10.0
$ ./bin/fesen
```

## Development

### Build

```
$ mvn install
```

### Build With Tests

```
$ mvn install
$ mvn package -P buildWithTests
```

### Build And Package

```
$ mvn package -P buildAndPackaging
```

