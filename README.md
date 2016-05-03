[![Build Status](https://travis-ci.org/ottogroup/flink-operator-library.svg?branch=master)](https://travis-ci.org/ottogroup/flink-operator-library)

# flink-operator-library

The library provides a set of [Apache Flink](https://flink.apache.org) operators and supporting utilities. Please visit the [wiki](https://github.com/ottogroup/flink-operator-library/wiki/) to learn more about the implemented components. Amongst them you will find 

* [a JSON based content filter](https://github.com/ottogroup/flink-operator-library/wiki/JSON-Content-Filter-operator)
* [an integration with flink-spector to allow testing of JSON based operators](https://github.com/ottogroup/flink-operator-library/wiki/JSON%20Content%20Matcher%20to%20integrate%20with%20Flink%20Spector%20for%20operator%20and%20pipeline%20testing)
* [JSON processing utilities](https://github.com/ottogroup/flink-operator-library/wiki/JSON%20processing%20utilities)
* [an abstract runtime foundation for streaming applications to speed up development](https://github.com/ottogroup/flink-operator-library/wiki/Base-runtime-for-streaming-applications)  
* [a selector implementation to extract keys from JSON documents](https://github.com/ottogroup/flink-operator-library/wiki/JSON-document-backed-key-selector) 
* [a JSON to character separated string converter](https://github.com/ottogroup/flink-operator-library/wiki/JSON-to-CSV-conversion-operator)
* [an operator to insert static content into existing JSON documents](https://github.com/ottogroup/flink-operator-library/wiki/Static-content-insertion-into-existing-JSON-documents)
* [a window-based JSON content aggregator](https://github.com/ottogroup/flink-operator-library/wiki/Window-based-JSON-content-aggregation)

To integrate the library with maven based projects, please add these sections:

```json
<repositories>
  <repository>
    <id>otto-bintray</id>
	<url>https://dl.bintray.com/ottogroup/maven</url>
  </repository>
</repositories>
``` 

```json
<dependencies>
  <dependency>
    <groupId>com.ottogroup.bi.streaming</groupId>
	<artifactId>flink-operator-library</artifactId>
	<version>0.3.3</version>  
  </dependency>
</dependencies>
```