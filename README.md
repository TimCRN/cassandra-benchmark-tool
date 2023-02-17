# Cassandra Benchmarking Tool
**MAIN**:&nbsp;[![Python application](https://github.com/TimCRN/cassandra-benchmark-tool/actions/workflows/python-app.yml/badge.svg?branch=main)](https://github.com/TimCRN/cassandra-benchmark-tool/actions/workflows/python-app.yml)&nbsp;&nbsp;&nbsp;&nbsp;
**DEV**:&nbsp;[![Python application](https://github.com/TimCRN/cassandra-benchmark-tool/actions/workflows/python-app.yml/badge.svg?branch=dev)](https://github.com/TimCRN/cassandra-benchmark-tool/actions/workflows/python-app.yml)

This tool is aimed towards a cluster of 8 Raspberry Pi's to generate benchmark results based on the given configuration from the user.<br /><br />

![benchmark-gif](https://user-images.githubusercontent.com/5428990/219660687-030f8201-d221-4aa3-ab85-e718081e34e3.gif)

## Current issues

At the moment, the code saves the total amount of each record it retrieves so that its deep size can be calculated when the run has finished. Initially, the deep size was calculated for each batch of records it had retrieved but in comparing results with and without calculation between batches, it showed quite an impact. For this reason it has been chosen to save the records in memory and calculate its size after the benchmark completed. The other caviat with this is that a lot of records can easily overflow in memory.<br /><br />
To solve both issues, a solution has been thought out. The total size will be calculated, again, between each batch, but the benchmark process will be paused when this is happening. An issue might arise that a simple benchmark of 100 million records with a batch size of 10000 may take a long time to complete because it's taking a lot of time to calculate between each batch, but this can be solved by implementing a treshold point of when the calculation should begin. This creates a balance between how much memory will be used to save an amount of data, and how frequently the calculation it executed, without impacting the final results at all.<br /><br />

