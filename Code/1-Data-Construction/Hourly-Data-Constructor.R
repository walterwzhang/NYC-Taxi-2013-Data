# Construct the Taxi Cab Data in the format:
# - Months
# -- days
# --- hours
# In each of these subfolders there will be a csv file

## Further altered so the trip_time_in_sec will be imputed from the drop off time and the pick up
## time to avoid data entry issues in the raw data.

# Load the packages
require(data.table)
require(parallel)
require(lubridate)
require(KernelKnn)

i   <-   1

base_date <- as_date("2012-12-31")

month_ind   <-   month(base_date + i)
day_ind     <-   day(base_date + i)

num_cores <- 6L

# Function to match the location of the last drop off to the street score data base
#  - Returns a list of the q-score and the distance of the street score data point to the
#    drop off location
streetscore_matcher <- function(test_location_DT, thread_count = 1L)
{
    # street_DT is pre-defined
    street_mat <- as.matrix(street_DT[, .(longitude, latitude)])
    test_mat   <- as.matrix(test_location_DT)
    knn_mat    <- knn.index.dist(street_mat, test_mat, k = 1,
                                 method = "euclidean", threads = thread_count)
    knn_index  <- knn_mat$test_knn_idx
    knn_dist   <- knn_mat$test_knn_dist
    return(list(street_DT[knn_index, get("q-score")],
                knn_dist))
}

year_ind <- 2013L
save_folder <- "...save_folder..."
save_month_date_folder <- paste0("Month-", month_ind, "/Day-", day_ind, "/")

# load the meta data (all medallions and hack_licenses in the 2013 data)
load("...medallion_license_location...")

# Get data location
fare_data_location   <-   "...fare_data_location..."
fare_data_files      <-   grep(".*csv$", dir(fare_data_location), perl = TRUE, value = TRUE)
trip_data_location   <-   "..._trip_data_location..."
trip_data_files      <-   grep(".*csv$", dir(trip_data_location), perl = TRUE, value = TRUE)


# Load the Street Score csv file
street_DT <- fread("...street_score_location...")

# Loop over the data eventually or run separate scripts (Running seperate scripts here)
fare_data_file <- grep(paste0(".*_", month_ind, ".csv$"),
                       fare_data_files,
                       perl = TRUE,
                       value = TRUE)
trip_data_file <- grep(paste0(".*_", month_ind, ".csv$"),
                       trip_data_files,
                       perl = TRUE,
                       value = TRUE)


# Read in the data
fare_DT <- fread(paste0(fare_data_location, fare_data_file), showProgress = FALSE)
trip_DT <- fread(paste0(trip_data_location, trip_data_file), showProgress = FALSE)


# Convert Dates to Date-time
NYC_timezone   <-   grep("New_York", OlsonNames(), perl = TRUE, value = TRUE)
fare_DT[, pickup_datetime    :=   as_datetime(pickup_datetime,  tz = NYC_timezone)]
trip_DT[, pickup_datetime    :=   as_datetime(pickup_datetime,  tz = NYC_timezone)]
trip_DT[, dropoff_datetime   :=   as_datetime(dropoff_datetime, tz = NYC_timezone)]

# Drop Values so we only focus on the specific day: ---

day_index_values <- which((day(trip_DT[,  pickup_datetime]) == day_ind) | (day(trip_DT[,  dropoff_datetime]) == day_ind))
fare_DT    <- fare_DT[day_index_values]
trip_DT    <- trip_DT[day_index_values] # Ensure same indices dropped

# General Cleaning -----


## Drop latitude and longtitude outside of NYC
### bounding box of NYC city limits is latitude=40.917577, longitude=-74.259090
### at the northwest corner, and latitude=40.477399, longitude=-73.700272 at the southeast corner.
### Ref: https://www.maptechnica.com/city-map/New%20York/NY/3651000

lat_lon_cleaning <- function(x, nw_bound = list(lat = 40.917577, lon = -74.259090),
                                se_bound = list(lat = 40.477399, lon = -73.700272))
{
    ind <- which(x$dropoff_longitude < nw_bound$lon | x$dropoff_longitude > se_bound$lon)
    x$dropoff_longitude[ind] <- NA
    ind <- which(x$pickup_longitude < nw_bound$lon | x$pickup_longitude > se_bound$lon)
    x$pickup_longitude[ind] <- NA
    ind <- which(x$dropoff_latitude < se_bound$lat | x$dropoff_latitude > nw_bound$lat)
    x$dropoff_latitude[ind] <- NA
    ind <- which(x$pickup_latitude < se_bound$lat | x$pickup_latitude > nw_bound$lat)
    x$pickup_latitude[ind] <- NA
    return(x)
}
trip_DT <- lat_lon_cleaning(trip_DT)

## Drop time = 0 and trip_distance = 0
trip_DT[, trip_time_in_secs := ifelse(trip_time_in_secs == 0, NA, trip_time_in_secs)]
trip_DT[, trip_distance     := ifelse(trip_distance == 0,     NA, trip_distance)]

## Drop NA data
complete_indicies <- complete.cases(trip_DT)
fare_DT    <- fare_DT[complete_indicies]
trip_DT    <- trip_DT[complete_indicies] # Ensure same indices dropped



hours_vec <- 0:23L # 24 hour time.

# Loop through the hours

out_list <- mclapply(hours_vec, function(hour_ind)
{
    save_hour_folder <- paste0("Hour-", hour_ind, "/")
    save_location <- paste0(save_folder, save_month_date_folder, save_hour_folder)
    dir.create(save_location, showWarnings = TRUE, recursive = TRUE)

    # Create boundary values for the time
    constructed_datetime_begin <- as_datetime(paste0(year_ind, "-", month_ind,
                                               "-", day_ind, " ",
                                               hour_ind, ":00:00"),  tz = NYC_timezone)
    constructed_datetime_end <- as_datetime(paste0(year_ind, "-", month_ind,
                                               "-", day_ind, " ",
                                               hour_ind, ":59:59"),  tz = NYC_timezone)

    constructed_interval <- constructed_datetime_begin %--% constructed_datetime_end

    # Get rides for that hour
    t1 = trip_DT[, pickup_datetime] %within% constructed_interval                                     # Trips start in the hour interval
    t2 = trip_DT[, dropoff_datetime] %within% constructed_interval                                    # Trips end in the hour interval
    t3 = constructed_interval %within% (trip_DT[, pickup_datetime] %--% trip_DT[, dropoff_datetime])  # Trips that encompass the interval
    datetime_ind <- which( t1 | t2 | t3)


    ind_trip_DT <- trip_DT[datetime_ind, ]
    ind_fare_DT <- fare_DT[datetime_ind, ]


    # Get weighting for the hour
    hour_portion_DT <- ind_trip_DT[, .(pickup_datetime, dropoff_datetime, trip_time_in_secs,
                                       dropoff_longitude, dropoff_latitude)]
    ## @NOTE: Imputation is done here
    hour_portion_DT[, trip_time_in_secs   :=   as.numeric(difftime(dropoff_datetime, pickup_datetime, units = "secs"))]

    # @NOTE: Fixed Issue to ensure pickup_outside and dropoff_outside are computed correctly
    hour_portion_DT[, pickup_outside      :=   ifelse(hour(pickup_datetime)  != hour_ind | day(pickup_datetime)  != day_ind | month(pickup_datetime)  != month_ind, 1L, 0L)]
    hour_portion_DT[, dropoff_outside     :=   ifelse(hour(dropoff_datetime) != hour_ind | day(dropoff_datetime) != day_ind | month(dropoff_datetime) != month_ind, 1L, 0L)]

    # summary(hour_portion_DT)
    # dim(hour_portion_DT)

    # Compute profit weights
    ## Take the 1 - (time spent outside interval)/(time trip) for fraction of time spent of trip
    ### Trip begins and ends in the hour interval
    hour_portion_DT[dropoff_outside == 0 & pickup_outside == 0, profit_weight := 1]
    ### Trip Dropoffs outside the interval
    hour_portion_DT[dropoff_outside == 1 & pickup_outside == 0, profit_weight := 1 - (as.numeric(dropoff_datetime) - as.numeric(constructed_datetime_end))/(as.numeric(dropoff_datetime) - as.numeric(pickup_datetime))]
    ### Trip Pickups outside the interval
    hour_portion_DT[dropoff_outside == 0 & pickup_outside == 1, profit_weight := 1 - (as.numeric(constructed_datetime_begin) - as.numeric(pickup_datetime))/(as.numeric(dropoff_datetime) - as.numeric(pickup_datetime))]
    ### Trip Pickup and Dropoff outside interval
    hour_portion_DT[dropoff_outside == 1 & pickup_outside == 1, profit_weight := (as.numeric(constructed_datetime_end) - as.numeric(constructed_datetime_begin))/(as.numeric(dropoff_datetime) - as.numeric(pickup_datetime))]


    # Compute Trip Time (weighted)
    hour_portion_DT[, trip_time_in_secs := floor(profit_weight * trip_time_in_secs)]

    ind_fare_subset_DT <- cbind(ind_fare_DT[,.(medallion, hack_license, fare_amount, tip_amount, total_amount)],
                                hour_portion_DT[, .(profit_weight, trip_time_in_secs,
                                                    dropoff_datetime, dropoff_longitude, dropoff_latitude)])
    # Weight the other values
    ind_fare_subset_DT[, fare_amount        :=   fare_amount * profit_weight]
    ind_fare_subset_DT[, tip_amount         :=   tip_amount * profit_weight]
    ind_fare_subset_DT[, total_amount       :=   total_amount * profit_weight]
    ind_fare_subset_DT <- ind_fare_subset_DT[, !c("profit_weight"), with = FALSE]

    # Aggregate Trip values
    ind_fare_agg_DT   <-   ind_fare_subset_DT[, list(fare               =   sum(fare_amount),
                                                     tip                =   sum(tip_amount),
                                                     totalearnings      =   sum(total_amount),
                                                     driving_time_sec   =   sum(trip_time_in_secs),
                                                     latest_datetime    =   max(dropoff_datetime),
                                                     latest_longitude   =   .SD[which.max(dropoff_datetime),  dropoff_longitude],
                                                     latest_latitude    =   .SD[which.max(dropoff_datetime),  dropoff_latitude]
                                                     ), by = c("medallion", "hack_license")]

    ## Add in Street Score
    if (nrow(ind_fare_agg_DT) > 0)
    {
        ind_fare_agg_dropoff_DT   <-   ind_fare_agg_DT[, .(latest_longitude, latest_latitude)]
        q_score_results           <-   streetscore_matcher(ind_fare_agg_dropoff_DT)
        q_score_vec               <-   q_score_results[[1]]
        q_score_dist              <-   q_score_results[[2]]
        ind_fare_agg_DT[, street_score      :=  q_score_vec]
        ind_fare_agg_DT[, street_score_dist :=  q_score_dist]
    } else
    {
        # Handle Month 3, Hour 3, Day 10 - Daylight Savings for 2013 Case
        ind_fare_agg_DT[, street_score      :=  character()]
        ind_fare_agg_DT[, street_score_dist :=  character()]
    }


    # Compute working flag
    ind_fare_agg_DT[, working := ifelse(totalearnings > 0, 1L, 0L)]

    setcolorder(ind_fare_agg_DT, c("medallion", "hack_license", "working",
                                   "fare", "tip", "totalearnings", "driving_time_sec", "latest_datetime",
                                   "latest_longitude", "latest_latitude",
                                   "street_score", "street_score_dist"))


    m_DT <- merge(medallion_ownership_DT, ind_fare_agg_DT, all.x = TRUE, by = c("medallion", "hack_license"))

    # Drop Medallion and Hack_License and use two indices instead to save space
    ## Company_Owned flag is also dropped to save space
    m_DT <- m_DT[,!c("medallion", "hack_license"), with = FALSE]

    # Save the data
    fwrite(m_DT, file = paste0(save_location, "data.csv"), sep = ",", row.names = FALSE, col.names = TRUE)
    Sys.time()
}, mc.cores = num_cores)

print(out_list)

print(paste0("Done! ", Sys.time()))
