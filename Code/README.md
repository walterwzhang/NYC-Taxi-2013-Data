# NYC Taxi 2013 Code README

Last Edited: March 31, 2020

This folder contains the code used to construct the data from the raw data files. The `Data-Construction` files should be run first and convert the data into hourly blocks by each driver. The `Data-Processing` scripts take the constructed data and generates the data following the final structure.

The current random sample of data, 5k and 100k drivers, have their index and medallions in the `Medallion-Samples` folder in the repository. Subsequent constructions of the data should use those drivers to ensure the constructed data will be the same.
