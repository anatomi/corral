# Introduction
Create EC2 instance with EFS mounted on it
Use EFS mount path to run the benchmark

# Command Line Arguments
Below are the command line arguments to the program (which can be displayed using -help):
```
    -path string
            Directory for writing&reading data from mounted EFS (default "/mnt/cache")
    -t int
            Number of threads to run (default 1)
    -z string
            Size of objects in bytes with postfix K, M, and G (default "1M")
    -d int
        Duration of each test in seconds (default 60)
```        
