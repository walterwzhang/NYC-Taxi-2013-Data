# NYC Taxi 2013 README

Authors: Walter W. Zhang and Øystein Daljord

Last Edited: April 1, 2020

This repository contains scripts that construct, process, and aggregate the 2013 NYC Taxi Data.

## Data structure

### `Sample_Data.csv`

`Sample_Data.csv` contains the information for the **individual owned** taxis' drivers in the period. We examine **six hour shifts** in this data set. The **45 minute shifts** can be built accordingly. We define **individual owned** taxis' drivers as taxi medallions with only one hack license. The data has the following columns:

`medallion_index`: Medallion Integer Index                                          (Integer)
`driver_index`: Driver Integer Index                                                (Integer)
`working`: Flag for if the driver is working this hour                              (1 if working, 0 if not)
`fare`: Base fare earned in that hour                                               (numeric)
`tip`: Tip for that hour                                                            (numeric)
`totalearnings`: total earnings in that hour                                        (numeric)
`start_45m`: Flag for if the driver started working in that hour (45 minute shift)  (1 if working, 0 if not)
`start_6h`: Flag for if the driver started working in that hour (6 hour shift)      (1 if working, 0 if not)
`quit_45m`: Flag for if the driver quit the 45 min shift that hour                  (1 if working, 0 if not)
`quit_6h`: Flag for if the driver quit the 6 hour shift that hour                   (1 if working, 0 if not)
`shift_45m`: Number of cumulative 45 min shifts the driver is on                    (integer)
`shift_6h`: Number of cumulative 6 hr shifts the driver is on                       (integer)
`shift_type_45m`: Dummy for 45 min shift                                            (integer)
`shift_type_6h`: Dummy for 6 hour shift                                             (integer)
`cumulative_driving_time_sec_45m`: Cumulative driving time for that 45 min *shift*  (numeric)
`cumulative_driving_time_sec_6h`: Cumulative driving time for that 6 hour *shift*   (numeric)
`dt`: The datetime in hour increments (NYT Time)                                    (datetime object)


For the `shift_type_45m` and `shift_type_6h`, the dummy breakdown is:

0. Driver has not started a shift yet
1. Driver has started a "Day Shift" (4 AM to 9:59 AM)
2. Driver has started a "Night Shift" (2 PM to 7:59 PM)
3. Driver has started a "Bohemian Shift" (10 AM to 1:59 PM or 8 PM to 3:59 AM)


Currently the *shift*, *shift_type*, and *cumulative_driving_time_sec* are left as a cumulative count. In other words, after a driver ends (or *quits*) a shift, the driver's *shift*, *shift_type*, and *cumulative_driving_time_sec* columns will still reflect (and be carried over to the next hour) the last values despite the driver not working. When the driver starts a new shift, the *shift* column will increment by one, the *shift_type* column will reset to reflect what type of shift it is, and the *cumulative_driving_time_sec* column will reset and reflect the cumulative driving time for the current shift.


### `medallion_mapping_key.csv`

The Medallion-Samples folder contains the medallions for the 5k and 100k driver samples. The medallion and hack_license mappings can be found in the `medallion_mapping_key.csv`. The mapping was done to reduce file size of the `data.csv` file. The data has the following columns

`medallion`: The Taxi's Medallion                                     (string)
`hack_license`: Driver's Hack License (can share a taxi medallion)    (string)
`medallion_index`: Medallion Integer Index                            (integer)
`driver_index`: Driver Integer Index                                  (integer)
`company_owned`: Flag for company ownership                           (1 if company owned and 0 if individually owned)


## Data cleaning

Before aggregation, all the observations with the following properties are dropped:

- Trip time is *0*
- Trip distance is *0*
- Trips with a drop off **and** pick up outside this box: [Ref](https://www.maptechnica.com/city-map/New%20York/NY/3651000)
    `nw_point = list(lat = 40.917577, lon = -74.259090)`
    `se_point = list(lat = 40.477399, lon = -73.700272)`
    This is a generous box around the tri-state area near NYC


## Data construction and processing

Scripts should be run in sequence by their numerical prefix. The first folder of scripts constructs the data from the raw data. The second folder of scripts processes the constructed data to the desired format. The first set of scripts should be run on a SLURM job scheduler and the second set of scripts should be run on a local machine. File paths should be set accordingly before a re-build of the data and the ellipsis in the scripts' file paths should be changed to reflect your computing environment. 

A HPC cluster is suggested for constructing the data construction step since it is an embarrassing parallel workload. A high memory machine (~250 GB RAM) is recommended for processing and aggregating the constructed data to create the final data samples.
