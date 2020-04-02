# Given the constructed Taxi Data, makes the processed data set
# Log file tracks the processing progress

# Load the packages
require(data.table)
require(lubridate)


# Set primitives
year_ind <- 2013L
save_folder <- "...Constructed_Data..."


# Flag for 5k, 20k, 100k, Whole Data
## 0L is whole data
## 1L is 5k driver sample data
## 2L is 20k driver sample data
## 3L is 100k driver sample data

build_flag <- 1L

if (build_flag == 1)
{
    med_DT  <- fread(".../Medallion_Sample_5k.csv")
    new_save_folder <- ".../NYC-Taxi-2013-Time-Imputed-5k/"
    log_file <- ".../taxi-log-5k.txt"
} else if (build_flag == 2)
{
    med_DT <- fread(".../Medallion_Sample_20k.csv")
    new_save_folder <- ".../NYC-Taxi-2013-Time-Imputed-20k/"
    log_file <- ".../taxi-log-20k.txt"
} else if (build_flag == 3)
{
    med_DT <- fread(".../Medallion_Sample_100k.csv")
    new_save_folder <- ".../NYC-Taxi-2013-Time-Imputed-100k/"
    log_file <- ".../taxi-log-100k.txt"
} else
{
    med_DT <- fread(".../medallion_mapping_key.csv")
    new_save_folder <- ".../NYC-Taxi-2013-Time-Imputed/"
    log_file <- ".../taxi-log.txt"
}
dir.create(new_save_folder, recursive = TRUE, showWarnings = FALSE)
med_DT_ind <- med_DT[, .(medallion_index, driver_index)][order(medallion_index, driver_index),]


paste0("Got DT Threads: ", getDTthreads())

base_date <- as_date("2012-12-31")

# Log for the Processor

cat("Results Processor Log", file = log_file, sep = "\n")

print(Sys.time())

day_shift_starting_hours_1   <- "4:00:00"
day_shift_starting_hours_2   <- "9:59:59"
night_shift_starting_hours_1 <- "14:00:00"
night_shift_starting_hours_2 <- "19:59:59"

old_data_DT        <-   data.table()
work_tracker_DT    <-   data.table()
on_a_shift_6h_DT   <-   data.table()

# Loop for i from 1 to 365...
init_time <- Sys.time()
for (i in 1:365)
{
    month_ind   <-   month(base_date + i)
    day_ind     <-   day(base_date + i)
    save_month_date_folder <- paste0("Month-", month_ind, "/Day-", day_ind, "/")

    # Create Day and Night Shift Intervals
    day_shift_starting_hours   <- ymd_hms(paste0(year_ind, "-", month_ind,
                                                 "-", day_ind, " ", day_shift_starting_hours_1)) %--%
                                  ymd_hms(paste0(year_ind, "-", month_ind,
                                                 "-", day_ind, " ", day_shift_starting_hours_2))
    night_shift_starting_hours <- ymd_hms(paste0(year_ind, "-", month_ind,
                                                 "-", day_ind, " ", night_shift_starting_hours_1)) %--%
                                  ymd_hms(paste0(year_ind, "-", month_ind,
                                                 "-", day_ind, " ", night_shift_starting_hours_2))

    # Loop over hours 0 to 23
    for (hour_ind in 0:23)
    {
        if (month_ind == 3 & day_ind == 10 & hour_ind == 2)
        {
            # Daylight Savings in March Case (3 AM/hour_ind=2 does not exist...)

            # Write to log file to document output
            cat(paste0(new_save_location, " Done! ", Sys.time()), file = log_file,
                append = TRUE, sep = "\n")
            next
        }

        save_location       <-   paste0(save_folder, save_month_date_folder, "Hour-", hour_ind, "/")
        new_save_location   <-   paste0(new_save_folder, save_month_date_folder, "Hour-", hour_ind, "/")
        dir.create(new_save_location, recursive = TRUE, showWarnings =  FALSE)
        iteration_datetime  <- ymd_hms(paste0(year_ind, "-", month_ind,  "-", day_ind, " ", hour_ind, ":00:00"))
        is_day_shift_hour   <- iteration_datetime %within% day_shift_starting_hours
        is_night_shift_hour <- iteration_datetime %within% night_shift_starting_hours

        data_DT <- fread(paste0(save_location, "data.csv"))

        ## Drop Drivers not in the subset if needed
        if (build_flag != 0)
        {
            # 5k Case or 20k Case
            data_DT <- merge(med_DT_ind, data_DT, all.x = TRUE, sort = FALSE,
                             by = c("medallion_index", "driver_index"))

        }


        # Fill the NA Values when applicable
        #  - Leave the street_score, street_score_dist, latest_longitude, latest_latitude
        #    and latest_datetime cols as NA.

        data_DT[is.na(working),          working            :=   0L]
        data_DT[is.na(fare),             fare               :=   0L]
        data_DT[is.na(tip),              tip                :=   0L]
        data_DT[is.na(totalearnings),    totalearnings      :=   0L]
        data_DT[is.na(driving_time_sec), driving_time_sec   :=   0L]


        if (month_ind == 1 & day_ind == 1 & hour_ind == 0)
        {
            ## First Day, First Month, First Hour Case (no past hour data)

            # Start Shift Working Flag
            data_DT[, start_45m := ifelse(working == 1, 1L, 0L)]
            data_DT[, start_6h  := ifelse(working == 1, 1L, 0L)]

            # No Quitting here
            data_DT[, quit_45m := 0L]
            data_DT[, quit_6h  := 0L]

            # Work Shift
            data_DT[, shift_45m := ifelse(working == 1, 1L, 0L)]
            data_DT[, shift_6h  := ifelse(working == 1, 1L, 0L)]

            # Classify Shift
            if (is_day_shift_hour)
            {
                # Day Shift Flag is 1
                data_DT[, shift_type_45m := ifelse(working == 1, 1L, 0L)]
                data_DT[, shift_type_6h  := ifelse(working == 1, 1L, 0L)]
            } else if (is_night_shift_hour)
            {
                # Night Shift Flag is 2
                data_DT[, shift_type_45m := ifelse(working == 1, 2L, 0L)]
                data_DT[, shift_type_6h  := ifelse(working == 1, 2L, 0L)]
            } else
            {
                # Other Shift Flag is 0
                data_DT[, shift_type_45m := ifelse(working == 1, 3L, 0L)]
                data_DT[, shift_type_6h  := ifelse(working == 1, 3L, 0L)]
            }

            # Cumulative Time Driving in a Shift
            data_DT[, cumulative_driving_time_sec_45m := driving_time_sec]
            data_DT[, cumulative_driving_time_sec_6h  := driving_time_sec]

            # Add to Work Tracker data.table
            work_tracker_DT    <-   data_DT[, .(working)]
            on_a_shift_6h_DT   <-   data_DT[, .(start_6h)]
        } else
        {
            ## Normal Case (old_data_DT is not empty)
            work_tracker_DT_ncols  <- ncol(work_tracker_DT)
            not_working_last_hour  <- (old_data_DT$working == 0)
            on_a_shift_6h          <- (on_a_shift_6h_DT$start_6h == 1)
            if (work_tracker_DT_ncols >= 5)
            {
                # @NOTE: FIXED HERE! Previously was counting working_last_5h...
                not_working_last_5h   <- (apply(work_tracker_DT, 1, sum) == 0) # Not working for last 5 hours
            } else
            {
                not_working_last_5h   <-  rep(FALSE, length(data_DT$working)) # Not enough info
            }

            # Start Trip Working Flag
            data_DT[, start_45m := ifelse(working == 1 & (not_working_last_hour), 1L, 0L)]
            data_DT[, start_6h  := ifelse(working == 1 & (not_working_last_5h),   1L, 0L)]

            # Classify Shift
            last_shift_type_45m <- old_data_DT$shift_type_45m
            last_shift_type_6h  <- old_data_DT$shift_type_6h

            if (is_day_shift_hour)
            {
                # Day Shift Flag is 1
                data_DT[, shift_type_45m := ifelse(start_45m == 1, 1L, last_shift_type_45m)]
                data_DT[, shift_type_6h  := ifelse(start_6h  == 1, 1L, last_shift_type_6h)]
            } else if (is_night_shift_hour)
            {
                # Night Shift Flag is 2
                data_DT[, shift_type_45m := ifelse(start_45m == 1, 2L, last_shift_type_45m)]
                data_DT[, shift_type_6h  := ifelse(start_6h  == 1, 2L, last_shift_type_6h)]
            } else
            {
                # Other Shift Flag is 0
                data_DT[, shift_type_45m := ifelse(start_45m == 1, 3L, last_shift_type_45m)]
                data_DT[, shift_type_6h  := ifelse(start_6h  == 1, 3L, last_shift_type_6h)]
            }


            # Quitting here
            data_DT[, quit_45m := ifelse(working == 0 & (!not_working_last_hour), 1L, 0L)]
            if (work_tracker_DT_ncols >= 5)
            {
                ## Normal Case
                data_DT[, quit_6h  := ifelse(working == 0 & (!not_working_last_5h) & on_a_shift_6h, 1L, 0L)]
                quit_shift <- (data_DT$quit_6h == 1)
                on_a_shift_6h_DT[quit_shift, start_6h := 0L]
            } else
            {
                ## Catch First Day, First Month, 1 AM - 4 AM Case (no data for past 5 hours)
                data_DT[, quit_6h  := 0L]
            }


            # Work Shift (Cumulative Working Shift)
            last_shift45m <- old_data_DT$shift_45m
            last_shift6h  <- old_data_DT$shift_6h

            data_DT[not_working_last_hour,  shift_45m := ifelse(working == 1,
                                                                last_shift45m[not_working_last_hour] + 1L,
                                                                last_shift45m[not_working_last_hour])]
            data_DT[!not_working_last_hour, shift_45m := last_shift45m[!not_working_last_hour]]

            if (all(!not_working_last_5h) == FALSE)
            {
                # everybody is (working in last 5 hours) is FALSE
                # aka someone is working in the last 5 hours
                data_DT[not_working_last_5h,  shift_6h := ifelse(working == 1 & (!on_a_shift_6h[not_working_last_5h]),
                                                                   last_shift6h[not_working_last_5h] + 1L,
                                                                   last_shift6h[not_working_last_5h])]
            }
            data_DT[!not_working_last_5h, shift_6h := last_shift6h[!not_working_last_5h]]


            # Cumulative Time Driving in a Shift
            previous_cumulative_driving_sec_45m   <-   old_data_DT$cumulative_driving_time_sec_45m
            previous_cumulative_driving_sec_6h    <-   old_data_DT$cumulative_driving_time_sec_6h
            currently_working_shift_45m           <-   (data_DT$quit_45m   == 0)
            currently_working_shift_6h            <-   (data_DT$quit_6h    == 0)
            currently_started_45m                 <-   (data_DT$start_45m == 1)
            currently_started_6h                  <-   (data_DT$start_6h  == 1)

            # data_DT[, driving_time_sec := as.numeric(driving_time_sec)]
            data_DT[currently_working_shift_45m & currently_started_45m,  cumulative_driving_time_sec_45m := driving_time_sec]
            data_DT[currently_working_shift_45m & !currently_started_45m, cumulative_driving_time_sec_45m := driving_time_sec + previous_cumulative_driving_sec_45m[currently_working_shift_45m & !currently_started_45m]]
            data_DT[!currently_working_shift_45m,                         cumulative_driving_time_sec_45m := previous_cumulative_driving_sec_45m[!currently_working_shift_45m]]


            data_DT[currently_working_shift_6h & currently_started_6h,  cumulative_driving_time_sec_6h := driving_time_sec]
            data_DT[currently_working_shift_6h & !currently_started_6h, cumulative_driving_time_sec_6h := driving_time_sec + previous_cumulative_driving_sec_6h[currently_working_shift_6h & !currently_started_6h]]
            data_DT[!currently_working_shift_6h,                        cumulative_driving_time_sec_6h := previous_cumulative_driving_sec_6h[!currently_working_shift_6h]]

            # Add to Work Tracker data.table
            if (work_tracker_DT_ncols >= 5)
            {
                ## Catch First Day, First Month, 1 AM - 4 AM Case (no data for past 5 hours)
                work_tracker_DT <- work_tracker_DT[, -1]
            }
            work_tracker_DT <- cbind(work_tracker_DT, data_DT[, .(working)])
            started_shift   <- (data_DT$start_6h == 1)
            on_a_shift_6h_DT[started_shift, start_6h := 1L]
        }

        data_DT <- data_DT[, !c("driving_time_sec"), with = FALSE]
        setcolorder(data_DT, c("medallion_index", "driver_index", "working",
                             "start_45m", "start_6h", "quit_45m", "quit_6h",
                             "shift_45m", "shift_6h",
                             "shift_type_45m", "shift_type_6h",
                             "cumulative_driving_time_sec_45m",
                             "cumulative_driving_time_sec_6h",
                             "fare", "tip", "totalearnings",
                             "latest_longitude", "latest_latitude",
                             "street_score", "street_score_dist"))

        old_data_DT <- copy(data_DT)
        fwrite(data_DT, file = paste0(new_save_location, "data.csv"))

        # Write to log file to document output
        cat(paste0(new_save_location, " Done! ", Sys.time()), file = log_file,
            append = TRUE, sep = "\n")
    }

    cat(paste0("Day ", i, " Done! ", Sys.time(), " ------------"), file = log_file,
        append = TRUE, sep = "\n")
}
end_time <- Sys.time()
cat(paste0("All Done! ", Sys.time()), file = log_file, append = TRUE, sep = "\n")

print(Sys.time())

print(paste0("Time Elapsed: ", format(end_time - init_time, digits = 3)))

# file.show(log_file)
