# Observability

Obervability is perhaps the most important aspect of any data pipeline. How can we be sure it is
working as expected if we can't see what is happening? 

For `blockbatch` processing  (and also for several other patterns at OP Labs) we introduce the idea
of data piepeline `markers`. This is simple: there is one marker for each batch, and we have separate
markers for each of the different data processing steps that apply to batches.

