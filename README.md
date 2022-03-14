<p align="left">
  <img src="https://github.com/databrickslabs/waterbear/blob/master/images/waterbear.png" width="300px"/>
</p>

# Waterbear

*Tardigrades can be found in milder environments such as lakes, ponds and meadows, often living near 'lakehouse'. 
Though these species are disarmingly cute, they are also nearly indestructible and can survive in harsh 
environments like outer space. This project gives life to the smallest fully functional data processing unit with the 
highest degrees of resilience and governance standards. We coded our tardigrades to carry the burden of 
enforcing enterprise data models that brings life to an industry regulated data lakehouse.*

___

[![DBR](https://img.shields.io/badge/DBR-9.1_ML-green)]()
[![codecov](https://codecov.io/gh/databrickslabs/waterbear/branch/master/graph/badge.svg)](https://codecov.io/gh/databrickslabs/watergrade)
[![PyPI version](https://badge.fury.io/py/dbl-waterbear.svg)](https://badge.fury.io/py/dbl-waterbear)

## Enterprise data models

Given an enterprise data model, we automatically convert an entity into its spark schema equivalent, extract metadata, 
derive tables expectations as SQL expressions and provision data pipelines that accelerate development of production 
workflows. Such a foundation allows financial services institutions to bootstrap their 
[Lakehouse for Financial Services](https://databricks.com/solutions/industries/financial-services) with 
resilient data pipelines and minimum development overhead. 

### JSON Schema

Adhering to strict industry data standards, our project is supporting data models expressed as 
[JSON Schema](https://json-schema.org/) and was built to ensure full compatibility with recent open source initiatives 
like the [FIRE](https://suade.org/fire/manifesto/) data model for regulatory reporting.
In the example below, we access the spark schema and delta expectations from the `collateral` entity.

```python
from waterbear.convertor import JsonSchemaConvertor
schema, constraints = JsonSchemaConvertor('fire/model').convert("collateral")
```

## Execution

Even though records may often "look" structured (e.g. reading JSON files or well-defined CSVs), 
enforcing a schema is not just a good practice; in enterprise settings, it guarantees any missing field is still 
expected, unexpected fields are discarded and data types are fully evaluated (e.g. a date should be treated as a date 
object and not a string). We retrieve the spark schema required to process a given entity that we can apply on batch 
or on real-time through structured streaming and auto-loader. In the example below, we enforce schema on a batch of 
CSV records, resulting in a schematized dataframe.

```python
derivative_df = (
    spark
        .read
        .format('csv')  # standard spark formats
        .schema(schema) # enforcing our data model
        .load('csv_files')
)
```

Applying a schema is one thing, enforcing its constraints is another. Given the schema definition of an entity, 
we can detect if a field is required or not. Given an enumeration object, we ensure its value consistency 
(e.g. country code). In addition to the technical constraints derived from the schema itself, the model also reports 
business expectations using e.g. `minimum`, `maximum`, `maxItems`, etc. 
All these technical and business constraints will be programmatically retrieved from our model and interpreted 
as a series of SQL expressions.

```json
{
  "[`high_fives`] VALUE": "`high_fives` IS NULL OR `high_fives` BETWEEN 1.0 AND 300.0",
  "[`id`] NULLABLE": "`id` IS NOT NULL",
  "[`person`.`username`] MATCH": "`person`.`username` IS NULL OR `person`.`username` RLIKE '^[a-z0-9]{2,}$'",
  "[`person`] NULLABLE": "`person` IS NOT NULL",
  "[`role`] VALUE": "`role` IS NULL OR `role` IN ('SA','CSE','SSA','RSA')",
  "[`skills`] SIZE": "`skills` IS NULL OR SIZE(`skills`) >= 1.0"
}
```

Although one could apply those expectations through simple user defined functions, we highly recommend
the use of [Delta Live Tables](https://databricks.com/product/delta-live-tables) to ensure both reliability and 
timeliness in financial data pipelines.

### Delta Live Tables

Our first step is to retrieve files landing to our industry lakehouse using Spark auto-loader. 
In continuous mode, news files will be processed as they unfold, `max_files` at a time. 
In triggered mode, only new files will be processed since last run. 
Using Delta Live Tables, we ensure the execution and processing of delta increments, preventing organizations 
from having to maintain complex checkpointing mechanisms.

```python
@dlt.create_table()
def bronze():
    return (
        spark
            .readStream
            .format('csv')   # we read standard sources
            .schema(schema)  # and enforce schema
            .convert('/path/to/data/files')
    )
```

Our pipeline will evaluate our series of SQL rules against our schematized dataset, 
flagging record breaching any of our expectations through the `expect_all` pattern and reporting on data quality 
in real time. 

```python
@dlt.create_table()
@dlt.expect_all(constraints) # we enforce expectations
def silver():
  return dlt.read_stream("bronze")
```

![](images/pipeline_processing.png)

## Testing

For integration testing, we also provide users with the ability to generate records that match a given schema and 
complies with basic expectations (pattern matching is not supported).

```python
from waterbear.generator import JsonRecordGenerator
xs = JsonRecordGenerator('fire/model').generate("collateral", 5)
for x in xs:
    print(x)
```

```json
{"id": 6867, "person": {"first_name": "vqgjldqqorklmupxibsrdyjw", "last_name": "vtsnbjuscbkvxyfdxrb", "birth_date": "2001-07-21"}, "skills": ["R"]}
{"id": 3119, "person": {"first_name": "vp", "last_name": "dgipl", "birth_date": "1972-03-23"}, "high_fives": 71, "skills": ["SCALA"]}
{"id": 4182, "person": {"first_name": "ijlzxxpv", "last_name": "ldpnnkohf", "birth_date": "1982-11-10"}, "joined_date": "2018-06-29", "skills": ["R"]}
{"id": 4940, "person": {"first_name": "lhklebpkcxp", "last_name": "jir", "birth_date": "1998-01-06"}, "high_fives": 213, "skills": ["SQL"], "role": "RSA"}
{"id": 5920, "person": {"first_name": "njadmuflxqbzc", "last_name": "arggdbaynulumrchreblfvxfe", "birth_date": "1997-06-26", "username": "snuafihfatyf"}, "high_fives": 105, "skills": ["PYTHON"], "role": "SA"}
```

## Building a wheel file

```shell
python setup.py bdist_wheel --universal
```

## Using the Project

```shell
pip install dbl-waterbear
```

## Project support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.

## Authors
<antoine.amend@databricks.com>
