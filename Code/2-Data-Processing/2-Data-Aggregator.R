# Aggregates the data from the data-processor script


# Load the packages
require(data.table)
require(parallel)
require(lubridate)


# Set primitives
year_ind    <- 2013L
num_cores   <- 50
save_folder <- "...Processed_Data..." # Processed data
paste0("Got DT Threads: ", getDTthreads())
setDTthreads(1L)
sample_data_save_location <- "...Processed_Data_Sample..." # Save location for sample data
save_memory_flag <- FALSE
cols_to_drop     <- c()

# Flag for 5k, 20k, Whole Data
## 0L is whole data
## 1L is 5k driver sample data
## 2L is 20k driver sample data
## 3L is 100k driver sample data

build_flag <- 1L


if (build_flag == 1)
{
    med_DT  <- fread(".../Medallion_Sample_5k.csv")
    new_save_folder <- ".../NYC-Taxi-2013-Time-Imputed-5k/"
    data_save_name <- "Sample_Data_5k.csv"
    data_save_name_zip <- "Sample_Data_5k.zip"
} else if (build_flag == 2)
{
    med_DT <- fread(".../Medallion_Sample_20k.csv")
    new_save_folder <- ".../NYC-Taxi-2013-Time-Imputed-20k/"
    data_save_name <- "Sample_Data_20k.csv"
    data_save_name_zip <- "Sample_Data_20k.zip"
}  else if (build_flag == 3)
{
    med_DT <- fread(".../Medallion_Sample_100k.csv")
    new_save_folder <- ".../NYC-Taxi-2013-Time-Imputed-100k/"
    data_save_name <- "Sample_Data_100k.csv"
    data_save_name_zip <- "Sample_Data_100k.zip"
    num_cores <- 1 # Prevent memory problems but will take a while
    save_memory_flag <- TRUE
    cols_to_drop <- c("latest_longitude", "latest_latitude",
                      "street_score", "street_score_dist",
                      "latest_datetime",
                      "medallion", "hack_license")
} else
{
    # Don't aggregate over all of the drivers
    stop("Don't aggregate over the whole data!")
}


# Base date
base_date <- as_datetime("2013-01-01 00:00:00")


# Loop over 365 * 24 - 1 hours
hours_vec <- 0L:(365L * 24L - 1L)

setDTthreads(1L)
init_time <- Sys.time()
selected_list <- mclapply(hours_vec, function(hour_index)
{
    new_date    <-   base_date + hours(hour_index)
    month_ind   <-   month(new_date)
    day_ind     <-   day(new_date)
    hour_ind    <-   hour(new_date)
    save_month_date_folder <- paste0("Month-", month_ind, "/Day-", day_ind, "/")
    new_save_location   <-   paste0(new_save_folder, save_month_date_folder, "Hour-", hour_ind, "/")
    if (file.exists(paste0(new_save_location, "data.csv")))
    {
        data_DT <- fread(paste0(new_save_location, "data.csv"), showProgress = FALSE)
        data_sub_DT <- merge(med_DT, data_DT, all.x = TRUE, by = c("medallion_index", "driver_index"))
        data_sub_DT[, dt := new_date]
        if (save_memory_flag)
        {
            # Drop columns to save memory
            data_sub_DT <- data_sub_DT[, !c(cols_to_drop), with = FALSE]
        }
        data_sub_DT
    } else
    {
        print(paste0("Hour Index: ", hour_index, " is empty!"))
        NULL
    }

}, mc.cores = num_cores)
end_time <- Sys.time()
print(paste0("Time Elapsed: ", format(end_time - init_time, digits = 3)))

## @NOTE: Hour lost from daylight savings (hour index 1634 will be missing)
## Date was constructed with this
# base_date + hours(1634)
## "2013-03-10 02:00:00 UTC"

# Bind the data together
selected_DT <- rbindlist(selected_list, use.names = TRUE)


if (build_flag == 1 | build_flag == 2)
{
    # Drop the unneeded columns
    selected_DT <- selected_DT[, !c("latest_longitude", "latest_latitude",
                                    "street_score", "street_score_dist",
                                    "latest_datetime"), with = FALSE]
    # Drop the medallion and hack_license as can merge in from the key
    selected_DT <- selected_DT[, !c("medallion", "hack_license"), with = FALSE]
}

format(object.size(selected_DT), units = "auto")
rm(selected_list); gc();

# Run Checks --------------------------------------------------------------------------------------

m_DT <- copy(selected_DT)
setDTthreads(num_cores)

if (build_flag == 1)
{
    # Manually construct the shift_45m (first approach)
    mm <- selected_DT[driver_index == 3 & medallion_index == 4557]$working
    r <- rle(mm)
    r$values <- cumsum(r$values) # * r$values
    sum(inverse.rle(r) - selected_DT[driver_index == 3 & medallion_index == 4557]$shift_45m)
    # Should be only 0s
    rm(r)

    # Manually construct the shift_45m (second approach)
    mm <- selected_DT[driver_index == 3 & medallion_index == 4557]$start_45m
    r <- rle(mm)
    r$values <- cumsum(r$values) # * r$values
    sum(inverse.rle(r) - selected_DT[driver_index == 3 & medallion_index == 4557]$shift_45m)
    # Should be only 0s
    rm(r)


    # Manually construct the  shift_6h
    mm <- selected_DT[driver_index == 3 & medallion_index == 4557]$start_6h
    r <- rle(mm)
    r$values <- cumsum(r$values)
    sum(inverse.rle(r) - selected_DT[driver_index == 3 & medallion_index == 4557]$shift_6h)
    # Should all be 0
    rm(r)

}



# Check shift_45m
m_DT[, shift_45m_new1 := { r <- rle(working)
                         r$values <- cumsum(r$values)
                         inverse.rle(r)
                        }, by = c("medallion_index", "driver_index")]
sum(m_DT[, shift_45m - shift_45m_new1])
# Should be summing to 0

m_DT[, shift_45m_new2 := { r <- rle(start_45m)
                         r$values <- cumsum(r$values)
                         inverse.rle(r)
                        }, by = c("medallion_index", "driver_index")]
sum(m_DT[, shift_45m - shift_45m_new2])
# Should be summing to 0



# Check shift_6h
m_DT[, shift_6h_new := { r <- rle(start_6h)
                         r$values <- cumsum(r$values)
                         inverse.rle(r)
                        }, by = c("medallion_index", "driver_index")]
sum(m_DT[, shift_6h - shift_6h_new])
# Should be summing to 0

rm(m_DT)

# -------------------------------------------------------------------------------------------------

# Save the data
fwrite(selected_DT, paste0(sample_data_save_location, data_save_name), nThread = num_cores)

# Zip the data from the command line
system(paste("zip", data_save_name_zip, data_save_name))

