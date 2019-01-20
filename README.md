# Cuckoo Filter

A thread safe probability filter that performs set membership tests.  A lookup returns either __might be in set__ or __definitely not in set__.

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.ikelin/cuckoofilter/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.ikelin/cuckoofilter)
[![Build Status](https://travis-ci.org/ikelin/cuckoofilter.svg?branch=master)](https://travis-ci.org/ikelin/cuckoofilter)

## Usage

```xml
<dependency>
    <groupId>com.ikelin</groupId>
    <artifactId>cuckoofilter</artifactId>
    <version>0.0.2</version>
</dependency>

```

## Creating a Filter

```java
CuckooFilter filter = CuckooFilter.create(10000)
    .withFalsePositiveProbability(0.001)
    .withConcurrencyLevel(8)
    .build();
```

This creates a cuckoo filter of with expected max capacity of `10,000`, false positive probability of `0.001` (or 0.1%), and concurrency level of `8`.

*Expected Max Capacity* specifies the expected number of items this set can hold.

*False Positive Probability* is the probability that lookup item operation will return a false positive.

The allowed concurrency among read and write operations is guided by *Concurrency Level*.

### Lookup Item

To lookup an item in the filter, use the `mightContain()` method:

```java
CuckooFilter tables = CuckooFilter.create(100).build();
boolean tableMightExist = tables.mightContain(tableHash)
if (!tableMightExist) {
  // table definitely does not exist, do not query database
}
```

If `mightContain()` returns `true`, the item might or might not be in the filter.  If `mightContain()` returns `false`, the item is definitely not in the set.
 
### Put Item

To put an item into the filter, use the `put()` method:

```java
CuckooFilter blacklistedWebsites = CuckooFilter.create(3000000).build();
boolean success = blacklistedWebsites.put(websiteHash);
if (!success) {
  // set expectedMaxCapacity reached...
}

```

Always check the `boolean` returned from the `put()` method.  If `put()` returns `true`, the item is successfully inserted. If `put()` returns `false`, the filter has reached its capacity.  In this case, create a new filter with larger capacity. 

### Remove Item

To remove an item from the filter, use the `remove()` method:

```java
CuckooFilter cdnCachedContents = CuckooFilter.create(100000000).build();
filter.remove(purgedCdnCachedContentHash);
```

## Item Hash Value

Use a performant hashing library that generates a well distributed long hash value for items.  For example, OpenHFT's [Zero Allocation Hash](https://github.com/OpenHFT/Zero-Allocation-Hashing) or Google's Guava [Hashing](https://github.com/google/guava/wiki/HashingExplained).

```java
LongHashFunction hashFunction = LongHashFunction.xx();
long itemHash = hashFunction.hashChars("item-foo");

if (!filter.mightContain(itemHash)) {
  // item definitely does not exist in filter...
}
```
