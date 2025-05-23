# Comparison to dbt and SQLMesh

In previous sections, we outlined our approach to transforming data in the warehouse. 
To summarize, we have two systems:

- [blockbatch load](./1-blockbatch-loading.md): optimized for onchain data ETL into the warehouse
- [transforms](./2-transforms.md): a more general-purpose transformation framework.

These systems are conceptually similar to open-source tools like dbt and SQLMesh, which naturally
raises the question: why didn’t we just use one of those?

We don't think dbt or SQLMesh are bad tools. In fact, they’re great for many teams. But for our
use case, where we value explicit control, tight database integration, and minimal abstraction, they
weren’t the right fit.

## What dbt and SQLMesh offer

Both dbt and SQLMesh are mature, well-documented frameworks designed to make data transformation
easier and more maintainable. Their main selling points include:

- Data lineage and dependency graphs.

- Testability and automated data quality checks.

- Orchestration support (via CLI or integrations).

- Abstractions over database DDL and model lifecycle.

These tools are widely adopted because they abstract many of the complexities of managing data
pipelines. But abstraction comes at a cost.


## Why we didn't chose dbt or SQLMesh

### Lineage

Lineage is arguably the most visible advantage. dbt users love seeing their data graph grow ever
more complex, and tracking how upstream data flows into downstream models. It feels powerful.

But in our experience, aside from the visual appeal, it does not offer practical benefits
for operating or maintaining the pipeline. When upstream data changes, tools like dbt or SQLMesh
can automatically recompute all downstream tables. But do you want to do that? What about compute
costs? Do you want to overwrite existing downstream tables or maybe create new versions so you
can compare changes? 

As a pipeline operator one has to make decisions when backfilling and it's rare when the answer
is free of subtleties. Nuanced understanding of the data and business logic is needed to make the
right choice. For that reason we prefer to make decisions explicitly instead of relying on
framework capabilities that can be powerful but are a black box.

### Testability

Testability and lineage are closely related. If I change something in an upstream model, which
downstream models am I going to break? To answer this question you need lineage and you need
a fast way to evaluate models to ensure everything will be OK. 

Our philosophy on this is that structural problems will manifest while you prototype or shortly
after you deploy (if you deploy a change without prototyping).  Semantic issues around the data
are not as easily preventable so you still have to watch out for them.

When codebases get bigger and there is not a single team but many teams collaborating with each
other then testability starts becoming non-negotiable. But at the scale of our team it is
something that we can live without.


### SQL First (no indirection) 

The CLI surface for dbt and SQLMesh is powerful, but that power can be a double-edged sword.
Invoking the right command requires deep knowledge of how your project is configured and how
scheduling is set up. It’s easy to end up spending time just figuring out how to run the "correct"
command or how to configure the project so that execution of a command results in the desired
action in the database.

Tools like dbt/SQLMesh reserve the right to talk to the database. This at first sounds good. But
when you are outside the happy path, and need to understand or debug exactly what's happening,
that extra layer stands in the way.

To support multiple engines, tools like dbt and SQLMesh are designed to work with a lowest common
denominator of database features. That works well for portability, but less so if you want to use
the more powerful features of a specific database engine to your advantage.

The above is specially true for a data warehouse like ClickHouse, where a lot of the data management
facilities are provided to you by the specific table engine that you pick when using a table.
When creating a table you may also want to have indexes or use special data types and codecs to
optimize compression. Or you may want to control how partitions or individual columns in a table
are expired.  You want to do this speaking directly to the database and not through a YAML
or JSON config.

Our approach centers around writing the exact CREATE TABLE statement that will be executed. There’s
no schema inference, no translation layer, and no assumption about what you want. If you want a
specific engine, codec, or index—just write it. This makes the system transparent, predictable, 
and lets you use all of the database's strengths. 


## When dbt and SQLMesh might be a better fit

At large enough organizations you start having two different personas in your data platform. 
Platform users, who understand the data and business logic and build the data pipelines, and
platform maintainers, who operate the shared frameworks, are in charge of orchestration, and have
a global view of the data platform.

When a team is small the two responsibilities blur and so you are better off minimizing the friction
and process involved. As team grows larger, platform maintainers need to worry about global resource
utilization and being able to have a common model for monitoring and executing jobs.

This is when large-scale frameworks like dbt and SQLMesh can be most helpful. They get in the
way of advanced platform users, but help beginner platform users avoid foot-guns and ensure a level
of uniformity across pipelines that is needed to keep global objectives and cost in check.
