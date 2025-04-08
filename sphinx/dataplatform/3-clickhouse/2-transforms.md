# Transforms: General Purpose Data Pipelines in ClickHouse

In the previous section we went over how to build data pipelines that process onchain data using
ClickHouse as the compute engine. In this section we talk about more general purpose transformations
that are meant to be used for a wider variety of use cases. 

The "transforms" system was developed before we built the blockbatch load system and so we also use
it to process onchain data. That said it would be a good idea to migrate some of the onchain data
tables from the transforms system to the blockbatch system. The blockbatch system offers better
data integrity guarantees and better monitoring.

## Transform Groups

A transform group is a collection of tables that are processed together. 


##


Hisorically general purpose transforms were developed before we built the blockbatch based data
pipelines