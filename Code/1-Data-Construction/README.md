# NYC Taxi 2013 Data Construction README

Last Edited: March 31, 2020

- These scripts should be run first and run on a SLURM job scheduler.
- The Python script will generate 365 jobs (and their respective scripts) to be run on the job scheduler. Each job will construct the hourly data for that day in parallel in R
- The Python reads in the R template R script and changes the index variable to the job index to reflect the different day's job index
- Saves the data used an integer index for the Medallions and the Driver to save space. The `medallion_mapping_key.csv` provides the conversion mapping
