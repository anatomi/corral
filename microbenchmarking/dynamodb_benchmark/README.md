# Introduction
Create Table with partition key, sort key and an additional "value" column
Create EC2 instance 

# Command Line Arguments
Below are the command line arguments to the program (which can be displayed using -help):
```
    -table string
            DynamoDB Table name
    -pk string
            Partitioning key of table
    -sk string
            Sort key of table
    -va string
            Value attribute of table (value column)
    -t int
            Number of threads to run (default 1)
    -z string
            Size of objects in bytes with postfix K, M, and G (default "1M")
    -d int
        Duration of each test in seconds (default 60)
```        
