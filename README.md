![](images/waterbear-small.png)

Though tardigrades are disarmingly cute, they are also nearly indestructible and can even survive in outer space.
This project creates the smallest fully functional unit of work with the highest degree of resilience and governance
standards that can be used to carry the work of enforcing industry data models on a data lakehouse.

## Enterprise data models

Given an enterprise data model, we programmed our waterbears to automatically convert each entity into its spark schema 
equivalent, extract metadata (such as field descriptions) and derive tables expectations as SQL expressions. 
This solid foundation allows financial services organizations to bootstrap their 
[Lakehouse for Financial Services](https://databricks.com/solutions/industries/financial-services) with 
fully functional pipelines, high governance standards and minimum development overhead. Designed with industry 
standards in mind, therefore compatible with multiple data formats and in line with the latest developments across 
the financial services industry.

### JSON Schema

Adhering to strict industry data standards, our project is supporting any data models expressed as 
[JSON Schema](https://json-schema.org/) and was built to ensure full compability with the 
[FIRE](https://suade.org/fire/manifesto/) initiative led by [Suade Labs](https://suade.org/).
The Financial Regulatory data standard (FIRE) defines a common specification for the transmission of granular data 
between regulatory systems in finance, supported by the [European Commission](http://ec.europa.eu/index_en.htm), 
the [Open Data Institute](http://opendata.institute/) and the [Open Data Incubator](https://opendataincubator.eu/). 
In the example below, we access the spark schema and delta expectations from the `collateral` entity.

```python
from lh4fs.schema import JsonBuilder
schema, constraints = JsonBuilder('fire/model').build("collateral")
```

### LEGEND Schema

[Open sourced by Goldman Sachs](https://www.finos.org/press/goldman-sachs-open-sources-its-data-modeling-platform-through-finos) 
and maintained by the [Finos](https://www.finos.org/) community, the [Legend](https://legend.finos.org/) framework 
is a flexible platform that offers financial institutions solutions to explore, define, connect and integrate data into 
their business processes. Through its abstraction language (PURE) and interface (legend studio), business modelers can 
collaborate in the creation to enterprise data models with strict governance standards and software delivery best 
practices. Pending our code contribution [approval](https://github.com/finos-labs/legend-delta) to the Finos community, 
we will access the spark schema and delta expectations from any PURE entity such as the `derivative` model.

```python
from lh4fs.schema import LegendBuilder
schema, constraints = LegendBuilder('legend/model').build("derivative")
```

## Execution

Even though records may often "look" structured (e.g. reading JSON files or well defined CSVs), 
enforcing a schema is not just a good practice; in enterprise settings, it guarantees any missing field is still 
expected, unexpected fields are discarded and data types are fully evaluated (e.g. a date should be treated as a date 
object and not a string). We retrieve the spark schema required to process a given entity that we can apply on batch 
or on real-time through structured streaming and auto-loader. In the example below, we enforce schema on a batch of 
CSV records, resulting in a schematized dataframe.

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
business expectations using e.g. minimum, maximum, maxItems, pattern JSON parameters or PURE business logic in Legend. 
All these technical and business constraints will be programmatically retrieved from our model and interpreted 
as a series of SQL expressions as per the following example.

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

Our first step is to retrieve files landing to a distributed file storage using Spark auto-loader 
(though this framework can easily be extended to read different streams, using a Kafka connector for instance). 
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
            .build('/path/to/data/files')
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

## Project support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.

## Authors
<antoine.amend@databricks.com>