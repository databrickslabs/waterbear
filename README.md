![](images/lakehouse-for-financial-services.jpeg)

*Given an enterprise data model, we programmatically convert each entity into its spark schema 
equivalent, extract metadata and derive tables expectations as SQL expressions. This critical foundation allows financial
services organizations to automatically provision 
[Lakehouse for Financial Services](https://databricks.com/solutions/industries/financial-services) with high governance
standards and minimum development overhead*

___

## Enterprise data models

This project was designed with industry standards in mind, therefore compatible with multiple data formats and in line
with the latest developments across the financial services industry.

### JSON Schema

Adhering to strict industry standards ([JSON Schema](https://json-schema.org/)), our project is compatible the 
[FIRE](https://suade.org/fire/manifesto/)initiative led by [Suade Labs](https://suade.org/) 
(although it would support any generic JSON schema).
The Financial Regulatory data standard (FIRE) defines a common specification for the transmission of granular data 
between regulatory systems in finance, supported by the European Commission, the Open Data Institute and 
the Open Data Incubator. 

<div class="image-group" style="width:100%; height:auto; margin:25px; text-align:center; background-color: white">
    <a href="" target="_blank">
        <img src="https://github.com/SuadeLabs/fire/raw/master/documentation/images/eu_commission.png" width="30%"/>
    </a>
    <a href="" target="_blank">
        <img src="https://github.com/SuadeLabs/fire/raw/master/documentation/images/odi.png" width="30%"/>
    </a>
    <a href="" target="_blank">
        <img src="https://github.com/SuadeLabs/fire/raw/master/documentation/images/odine.png" width="30%"/>
    </a>
</div>

We can easily read spark schema and delta expectations from our `collateral` entity.

```python
from lh4fs.schema import JsonBuilder
schema, constraints = JsonBuilder('fire/model').build("collateral")
```

### LEGEND Schema

Open sourced by Goldman Sachs and maintained by the FINOS community, the [Legend](https://legend.finos.org/) framework 
is a flexible platform that offers financial institutions solutions to explore, define, connect and integrate data into 
their business processes. Through its abstraction language (PURE) and interface (legend studio), business modelers can 
collaborate in the creation to enterprise data models with strict governance standards and software delivery best 
practices. 

<div class="image-group" style="width:100%; height:auto; margin:25px; text-align:center; background-color: white">
    <a href="" target="_blank">
        <img src="https://www.finos.org/hubfs/press-release-goldman-sachs-finos-legend-1.png" width="30%"/>
    </a>
</div>

Pending code [approval](https://github.com/finos-labs/legend-delta), the LEGEND data model will be fully 
supported, reading entities as follows. 

```python
from lh4fs.schema import LegendBuilder
schema, constraints = LegendBuilder('legend/model').build("derivative")
```

## Execution

Even though records may sometimes "look" structured (e.g. JSON files), enforcing a schema is not just a good practice; 
in enterprise settings, it guarantees any missing field is still expected, unexpected fields are discarded and data 
types are fully evaluated (e.g. a date should be treated as a date object and not a string). 
We retrieve the spark schema required to process a given entity (e.g. collateral, derivative) 
that we apply on batch or on real-time (e.g. over a stream of raw records).

```
StructField(id,IntegerType,false)
StructField(person,StructType(List(StructField(first_name,StringType,true),StructField(last_name,StringType,true),StructField(birth_date,DateType,true),StructField(username,StringType,true))),false)
StructField(joined_date,DateType,true)
StructField(high_fives,DoubleType,true)
StructField(skills,ArrayType(StringType,true),true)
StructField(role,StringType,true)
```

Given the above, one can enforce schema through native spark operations

```python
_ = (
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
business expectations using e.g. minimum, maximum, maxItems, pattern JSON parameters. 
All these technical and business constraints will be programmatically retrieved from our JSON model and interpreted 
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

### Delta Live Tables

Our first step is to retrieve files landing to a distributed file storage using Spark auto-loader 
(though this framework can easily be extended to read different streams, using a Kafka connector for instance). 
In continuous mode, files will be processed as they land, `max_files` at a time. 
In triggered mode, only new files will be processed since last run. 
Using Delta Live Tables, we ensure the execution and processing of delta increments, preventing organizations 
from having to maintain complex checkpointing mechanisms to understand what data needs to be processed next; 
delta live tables seamlessly handles records that haven't yet been processed, first in first out.

```python
@dlt.create_table()
def bronze():
    return (
        spark
            .readStream
            .format('XXX')  # we read standard data sources, json, csv, jdbc, etc.
            .schema(schema)  # ... but enforce schema
            .build('/path/to/data/files')
    )
```

Our pipeline will evaluate our series of SQL rules against our schematized dataset (i.e. reading from Bronze), 
dropping record breaching any of our expectations through the `expect_all` pattern and reporting on data quality 
in real time

```python
@dlt.create_table()
@dlt.expect_all(constraints) # we enforce expectations and may drop record, ignore or fail pipelines
def silver():
  return dlt.read_stream("bronze")
```

![](images/pipeline_processing.png)

## Project support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.

## Authors
<antoine.amend@databricks.com>