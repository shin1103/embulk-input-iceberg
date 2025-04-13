# Iceberg input plugin for Embulk

embulk-input-iceberg is the Embulk input plugin for Apache Iceberg.

## Overview
Required Embulk version >= 0.11.5.  
Java 11. iceberg API support Java 11 above. (Despite Embulk official support is Java 8)

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration
Now Only support REST Catalog, and MinIO Storage.

- **namespace** catalog namespace name (string, required)
- **table** catalog table name (string, required)
- **catalog_type** catalog type. use "REST" (string, required)
- **uri** catalog uri. if "REST" use http URI scheme  (string, required)
- **warehouse_location** warehouse to save data. if use S3, URI scheme.  (string, required)
- **file_io_impl** implementation of file io.  (string, required)
- **endpoint**: Object Storage endpoint. if set path_style_access true, actual path is  like "http://localhost:9000/warehouse/" (string, required)
- **path_style_access**: use path url (string, required)
- **table_filters**: filter rows. support filter is predicate expressions only. [expressions](https://iceberg.apache.org/docs/1.8.1/api/#expressions) (list, optional)
- **columns**: select column name list. if not define, all columns are selected.  (list, optional)

## Example
1. Select All rows and columns.
```yaml
in:
  type:
    source: maven
    group: io.github.shin1103
    name: iceberg
    version: 0.0.2
  namespace: "n_space"
  table: "my_table_2"
  catalog_type: "rest"
  uri: "http://localhost:8181"
  warehouse_location: "s3://warehouse/"
  file_io_impl: "org.apache.iceberg.aws.s3.S3FileIO"
  endpoint: "http://localhost:9000/"
  path_style_access: "true"
```

2.  Select particular columns only.
```yaml
in:
  type:
    source: maven
    group: io.github.shin1103
    name: iceberg
    version: 0.0.2
  namespace: "n_space"
  table: "my_table_2"
  catalog_type: "rest"
  uri: "http://localhost:8181"
  warehouse_location: "s3://warehouse/"
  file_io_impl: "org.apache.iceberg.aws.s3.S3FileIO"
  endpoint: "http://localhost:9000/"
  path_style_access: "true"
  columns:
    - id
    - name
```

3. Filter rows
Multi filter is available.
```yaml
in:
  type:
    source: maven
    group: io.github.shin1103
    name: iceberg
    version: 0.0.2
  namespace: "n_space"
  table: "my_table_2"
  catalog_type: "rest"
  uri: "http://localhost:8181"
  warehouse_location: "s3://warehouse/"
  file_io_impl: "org.apache.iceberg.aws.s3.S3FileIO"
  endpoint: "http://localhost:9000/"
  path_style_access: "true"
  table_filters:
    - {type: ISNULL, column: id}
    - {type: EQUAL, column: id, value: 4}
    - {type: GREATERTHAN, column: id, value: 2}
    - {type: IN, column: id, in_values: [2, 3]}
```
## Types
Types are different from [iceberg](https://iceberg.apache.org/spec/#primitive-types) and [Embulk](https://www.embulk.org/docs/built-in.html).
So some iceberg type aren't supported.

Unsupported Iceberg Types
- UUID
- FIXED
- BINARY
- STRUCT
- LIST
- MAP
- VARIANT
