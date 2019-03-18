#' Code that allows scoring against a model.
#'
#' This is the function that is deployed against an endpoint
#' and returns a base64 encoded json string with the resulting score of
#' whether or not the pipe is clogged.
#'
#' @param payload_json64enc This is a base64 encoded string of a JSON string that can be destructured and cast to R objects and variables.
#'
#' @return a base64 encoded JSON structure.
service <- function(payload_json64enc) {
  # Record service version here. -----------------------------------------------
  ..version.. <- "0.3"
  
  # Set up libraries -----------------------------------------------------------
  library(dplyr)
  library(purrr)
  library(stringr)
  library(lubridate)
  library(jsonlite)
  library(caTools)
  library(rapportools)
  
  # Unpack JSON to local object ------------------------------------------------
  payload <- payload_json64enc %>% base64_dec() %>% rawToChar() %>% fromJSON()
  # str(payload)
  
  
  # Transform data inputs ------------------------------------------------------

  # convert Unix epoch to date
  for ( k in c("last_valid_line_start_timestamp", "last_valid_line_end_timestamp")) {
    if (payload[[c("parameters", k)]] != " ") {
      payload[[c("parameters", k)]] <- payload[[c("parameters", k)]] %>% as_datetime() %>% as_date() %>% strftime("%F")
    }
  }
  

  
  
  # ============================================================================
  # Implement code here
  # ============================================================================
  
  # Note that the input object is in `payload`
  
  #' Remove outliers.
  #' 
  #' Code that will set outlier values to NA values.
  #' 
  #' This function takes an one dimensional array and find the location of
  #' outliers which are below M percentile-inter quantile range and above
  #' N percentile + inter quantile range). These values are replaced with NA
  #' (Here M=0.1 and N=0.9)
  #'
  #' Qunatile first sort the input data after ignoring NAs. Then 'q' quantile
  #' value in data x of size 'N' is given by value at q(N+1) position in data
  #' x. If quantile is between the i and jth observation, x[i] and x[j], and
  #' can be estimated by x[i] + (x[j] - x[i]) times (q*(N+1) - j). Once
  #' quantile values are computed, then we compute inter quantile range that is
  #' difference between values at M and N quantiles. In next step we replace
  #' values less than M quantile value minus inter quantile range with NAs and
  #' also replace values greater than N quantile value minus inter quantile
  #' range with NAs
  #'
  #' @param x Numeric.  Input data in which we need to find outleirs and replace them with NA.
  #' @param na.rm Boolean.  When na.rm is TRUE code does not consider NA values in x. Code takes values without NA's. If Na's are present in data always put na.rm = TRUE.
  #'
  #' @return y code output size is same as input x and wherever outliers are present, they are replaced by NA
  removeOutliers <- function(x, na.rm = TRUE, ...) {
    qnt <- quantile(x, probs=c(.1, .90), na.rm = na.rm, ...)
    H <- 1.5 * IQR(x, na.rm = na.rm)
    y <- x
    y[x < (qnt[1] - H)] <- NA
    y[x > (qnt[2] + H)] <- NA
    y
  }
  
  
  #' Remove high-frequency noise.
  #' 
  #' This smooths data and removes high-frequency noise from data.
  #' 
  #' This function is used to remove hig frequency noise using mean filtering
  #' and remove speckle noise using median filter
  #'
  #' This function takes one dimensional array data. Then we check for missing
  #' values. If there any missing values then we remove them from data and
  #' smooth the data. then we replace missing values in their positions in
  #' original data  and smoothed data in the original positions of unsmoothed
  #' data. incase the model_do_interpolation     is TRUE, then we use linear interpolation
  #' to fill the missing data.
  #'
  #' @param parameters List.  Of same structure as in input JSON 'parameter' object.  Parameters used in computations, fed in to main function.
  #' @param input_data DataFrame.  InputData is one dimensional array of current cycle
  #'
  #' @return input_data - this gets changed only when there are missing values otherwise it is same as original data
  DataSmoothening <- function(parameters, input_data) {
    missing_data_index <- which(is.na(input_data) == TRUE)
    valid_data_index <- which(is.na(input_data) == FALSE)
    
    if(parameters$model_data_smoothening_loess==0)
    {
      if (length(missing_data_index) > 0) {
        valid_data <- input_data[-missing_data_index]
        
        #   model_filter_window_size   - used to filter data based on mean filtering and median filtering
        #   model_do_interpolation     - when it is TRUE, we fill the missing data using linear interpolation
        smoothed_valid_data         <- runmed(runmean(na.omit(valid_data), parameters$model_filter_window_size  ), parameters$model_filter_window_size  )
        temp_data                   <- input_data
        temp_data[valid_data_index] <- smoothed_valid_data
        
        if (parameters$model_do_interpolation) {
          temp_data <- na.interpolation(temp_data, "linear")
        }
        input_data <- temp_data
      }else{
        input_data  <- runmed(runmean(input_data, parameters$model_filter_window_size  ), parameters$model_filter_window_size  )
      }
      
    }else{
      #http://r-statistics.co/Loess-Regression-With-R.html
      index = 1:length(valid_data_index)
      loessMod10 <- loess(input_data[valid_data_index] ~ index) 
      input_data[valid_data_index] <- predict(loessMod10)
    }
    
    return(input_data)
  }
  
  
  
  #' Check linearity of data.
  #' 
  #' Checks the linearity of a 1D array of time series data.
  #' 
  #' checkLinearity function takes one dimensional array check whether the data is linear
  #' or not. If data is linear it gives slope and intecept of linear line
  #' otherwise it returns NAs in place of slope and intercept.
  #' 
  #' checkLinearity function takes complete input data up to current time stamp (or
  #' currentIndex) and then remove outliers. Next remove non-linear noise using
  #' median filtering and high frequency noise using mean filtering. Compute the
  #' eigen values of the filtered data by using their index values and filtered
  #' data. We get 2 eigen values here as data and idex values fom 2 dimensional
  #' array. For linear line first eigen value is always high and second value
  #' is very small. In our case we put threshold on second eigen value. If
  #' second eigen value is greater than model_eigen_threshold    then data is not following
  #' linear trend. At the same time we check whether data is showing positive
  #' trend (positive slope). If it shows negative slope then we don't fit line
  #' to that data
  #' 
  #' The values of the vector in the returned value are:
  #'    slope for linear fit whenever length of valid data greater than parameters$minimumNoOfDaysDataForModel otherwise slope is NAN
  #'    
  #'    intercept for linear whenever length of valid data greater than parameters$minimumNoOfDaysDataForModel otherwise slope is NAN
  #'    
  #'    model_linear_trend_deviate_count is zero whenever linear fit has positive slope and second eigen value is less than parameters$model_eigen_threshold otherwise model_linear_trend_deviate_count keeps on incrementing by one
  #'    
  #'    last_valid_line_slope is slope of last valid line.
  #'    
  #'    last_valid_line_intercept is intecept of last valid line.
  #'    
  #'    outlier_removed_nabt is NABT value after applying outlier removal
  #' 
  #' @param parameters We need some of the predefined variables from parameters for checkLinearity function - model_filter_window_size,model_regression_min_run,model_eigen_threshold   
  #' @param input_data inputdata is cycle data up to currentIndex
  #' 
  #' @return vector of values, discussed in documentation.
  checkLinearity <- function(parameters, input_data,line_segment_start_index) {
    
    # Find indexes of non-missing input data.
    valid_data_index <- which(is.na(input_data) == FALSE)
    
    index_val <- seq(line_segment_start_index, length(input_data))
    datt      <- input_data[index_val]
    
    #computes number consecutive missing values in the current segment data
    #if number of missing values are greater than a threshold then we discard the current segment data and look for next new data to fit next line segment
    rl <- rle(is.na(datt))
    
    if(any(!is.empty(rl$values))&&length(rl$lengths)>1)
    { 
      if(max(rl$lengths[rl$values])>parameters$model_max_consecutive_missing_data_tolerance)
      {
        parameters$model_linear_trend_deviate_count = parameters$model_linear_trend_run_threshold+1
        return(c( "intercept"                                    = NaN
                  , "slope"                                      = NaN
                  , "model_linear_trend_deviate_count"           = parameters$model_linear_trend_deviate_count
                  , "last_valid_line_slope"                      = parameters$last_valid_line_slope
                  , "last_valid_line_intercept"                  = parameters$last_valid_line_intercept
                  , "current_line_number"                        = parameters$current_line_number
                  , "outlier_removed_nabt"                       = NaN
                  , "smoothed_nabt"                              = NaN
                  ,"line_computed_for_model_regression_min_run"  = parameters$line_computed_for_model_regression_min_run
                  ,"all_slopes"                                  = NaN
                  ,"all_intercepts"                              = NaN
        ))
      }
    }
    
    
    # Outlier removal code require minimum 3 valid points
    if(length(valid_data_index) > 2) {
      # Remove outliers from NABT
      input_data <- removeOutliers(input_data)
      #if we have more than two continuous outliers and values are less than mean NABT the it is assumed as shutdown and start a new line segment
      
      
      temp_data<- input_data[valid_data_index]
      temp_data_outlier_removed <- removeOutliers(temp_data)
      
      #if we observe consecutively 2 or more outliers and they are below the mean NABt value, then we discard the current line segment data and start looking for new line from next points
      
      if(!is.na(temp_data[length(temp_data)])&& !is.na(temp_data[(length(temp_data)-1)]))
      {
        if(is.na(temp_data_outlier_removed[length(temp_data)]) && is.na(temp_data_outlier_removed[(length(temp_data_outlier_removed)-1)]) && temp_data[length(temp_data)] < mean(temp_data,na.rm=TRUE)&&temp_data[(length(temp_data)-1)] < mean(temp_data,na.rm=TRUE))
        {
          parameters$model_linear_trend_deviate_count = parameters$model_linear_trend_run_threshold+1
          return(c( "intercept"                                    = NaN
                    , "slope"                                      = NaN
                    , "model_linear_trend_deviate_count"           = parameters$model_linear_trend_deviate_count
                    , "last_valid_line_slope"                      = parameters$last_valid_line_slope
                    , "last_valid_line_intercept"                  = parameters$last_valid_line_intercept
                    , "current_line_number"                        = parameters$current_line_number
                    , "outlier_removed_nabt"                       = NaN
                    , "smoothed_nabt"                              = NaN
                    , "line_computed_for_model_regression_min_run" = parameters$line_computed_for_model_regression_min_run
                    , "all_slopes"                                 = NaN
                    , "all_intercepts"                             = NaN
          ))
        }
      }
    }
    
    # Store the NABT value after checking for outlier condition.
    # TODO: Mahesh.  If I am reading this right, this is returning just the last NABT that is not missing
    #       after removing outliers.  Correct? - Yes
    outlier_removed_nabt <- input_data[length(input_data)]
    
    # Data filtering can be done only when valid data is greater than or equal to filter size
    valid_data_index <- which(is.na(input_data) == FALSE)
    if(length(valid_data_index) >= parameters$model_filter_window_size) {
      # smooth the data (median filtering + Mean filtering), Valid data should be greater than or equal to model_filter_window_size
      input_data <- DataSmoothening(parameters, input_data)
    }
    
    # We check for linearity on current segment data+new data. We don't use complete cycle data
    
    index_val <- seq(line_segment_start_index, length(input_data))
    datt      <- input_data[index_val]
    
    # get the length of valid data
    data_valid       <- na.omit(datt)
    data_valid_index <- which(is.na(datt) == FALSE)
    smoothed_nabt <- if (length(datt) > 0) {datt[length(datt)]} else {NaN}
    #smoothed_nabt <- if (length(data_valid_index) > 0) {data_valid[length(data_valid_index)]} else {NA}
    
    slope            <- NaN
    intercept        <- NaN
    all_intercepts   <- NaN
    all_slopes       <- NaN
    
    if (length(data_valid) > parameters$model_regression_min_run) {
      # create a matrix with 2 rows (first row time axis co-ordinate and second row is data values)
      data_for_eigen_Computation <- cbind(data_valid_index, data_valid)
      # compute the eigen values
      second_eigen_Val <- (princomp(data_for_eigen_Computation )$sd^2)[[2]]
      
      # Fit the line to the data
      tfit      <- lm(datt ~ index_val)
      slope     <- summary(tfit)$coefficients[2]
      intercept <- summary(tfit)$coefficients[1]
      all_slopes<-slope
      all_intercepts <-intercept
      
      #we fit line to first "parameters$model_regression_min_run" points and use same slope for all the new points uintil they satisfy lineraity condition
      #If line is fitted for the first time to the required minimum number of points then we need to store them, we use same slopes and intercepts for new points including fist fitted points until follwo linearity
      if(parameters$line_computed_for_model_regression_min_run==0)
      {
        if( slope>0 && second_eigen_Val <= parameters$model_eigen_threshold ) {
          
          parameters$last_valid_line_slope     <- slope
          parameters$last_valid_line_intercept <- intercept
          parameters$line_computed_for_model_regression_min_run = 1
        }else{
          #if model_regression_min points are availble but they are not showing linearity, then we discard those points and look for new data
          #here we set parameters$line_computed_for_model_regression_min_run to -1, this helps in setting slope and intercept 'NaN' later
          parameters$line_computed_for_model_regression_min_run = -1
        }
        
      }else{
        #once the line is fitted then we just check for linearity condition. if linearity persists then use the slope and intercept from the intial minimum points for this line as current slope and intercept
        if(parameters$line_computed_for_model_regression_min_run != -1)
        {
          
          #check for % change in slope. if slope change is greater than 'threshold' with in current line segment then we update slope otherwise we keep on using previous slope of same line number
          if(slope>parameters$last_valid_line_slope){
            percentage_slope_change = 100*((slope-parameters$last_valid_line_slope)/slope)
          }else{
            percentage_slope_change = 100*((parameters$last_valid_line_slope-slope)/parameters$last_valid_line_slope)
            
          }
          if(percentage_slope_change>parameters$model_max_percentage_slope_change_tolerance && slope>0){
            
            parameters$last_valid_line_slope     <- slope
            parameters$last_valid_line_intercept <- intercept
          }
          
        }else{
          #parameters$line_computed_for_model_regression_min_run is -1, then we don't have perfect fit. so we set slope and intercept to NaN
          slope = NaN
          intercept = NaN
        }
      }
      
      
      # when slope is positive and data is showing linear trend we update last positive slope and last intercept
      # if( slope>0 && second_eigen_Val <= parameters$model_eigen_threshold ) {
      # 
      #   parameters$last_valid_line_slope     <- slope
      #   parameters$last_valid_line_intercept <- intercept
      # }
      
      # check whether slope is positive and also second eigen value less than model_eigen_threshold   , then we keep on adding those points to
      # the array and keep computing model_eigen_threshold    and slope. Otherwise see how many times it break this consition continuously.
      
      
      if(parameters$line_computed_for_model_regression_min_run==1) {
        #when parameters$line_computed_for_model_regression_min_run is 1 then slope cannot be NaN , so below condition won't cause code failure
        if (second_eigen_Val > parameters$model_eigen_threshold || slope < 0) {
          parameters$model_linear_trend_deviate_count  =  parameters$model_linear_trend_run_threshold+1 #parameters$model_linear_trend_deviate_count  + 1
          # when line is not showing linear trend or negative slope we take current slope and intercept as last positive slope and its last intecept
          slope     <- parameters$last_valid_line_slope
          intercept <- parameters$last_valid_line_intercept
        }else {
          slope     <- parameters$last_valid_line_slope
          intercept <- parameters$last_valid_line_intercept
          parameters$model_linear_trend_deviate_count <- 0  # if model_linear_trend_deviate_count  is zero, data is showing linear trend
        }
      }
      #when parameters$line_computed_for_model_regression_min_run is -1 then we need to look for new batch of data as the current batch of data is not linear
      if(parameters$line_computed_for_model_regression_min_run==-1)
      {
        parameters$model_linear_trend_deviate_count <- parameters$model_linear_trend_run_threshold+1
      }
    }
    
    
    return(c( "intercept"                                   = intercept
              , "slope"                                      = slope
              , "model_linear_trend_deviate_count"           = parameters$model_linear_trend_deviate_count
              , "last_valid_line_slope"                      = parameters$last_valid_line_slope
              , "last_valid_line_intercept"                  = parameters$last_valid_line_intercept
              , "current_line_number"                        = parameters$current_line_number
              , "outlier_removed_nabt"                       = outlier_removed_nabt
              , "smoothed_nabt"                              = smoothed_nabt
              ,"line_computed_for_model_regression_min_run"  = parameters$line_computed_for_model_regression_min_run
              ,"all_slopes"                                  = all_slopes
              ,"all_intercepts"                              = all_intercepts
    ))
  }
  
  
  
  #' Piece-wise linear segment fit
  #' 
  #' This function takes paramters and current batch data (from starting of cycle to current time stamp).It gives us the line parameters as ouput.
  #' It sends data to checkLinearity function and get the linear fit details
  #' when we start analysing data for linearity after break point, initally data is not sufficient for linear fit. At this time we use previous line as valid line 
  #' when model_linear_trend_deviate_count is greater than model_linear_trend_run_threshold value then it is assumed that data is no longer linear and we reached break point. We reset model_linear_trend_deviate_count to zero and store all parameters of line just before break point
  #' 
  #' TODO Mahesh: ... and does what with it?  Add more documentation.
  #' 
  #' Parameters used:
  #' 
  #'   model_linear_trend_deviate_count  incremented if data is not linear
  #'   
  #'   last_valid_line_slope is updated once linear line break point is reached or model_linear_trend_deviate_count  is greater than model_linear_trend_run_threshold 
  #'   
  #'   last_valid_line_intercept is updated once linear line break point is reached or model_linear_trend_deviate_count  is greater than model_linear_trend_run_threshold 
  #'   
  #'   line_segment_start_index is set to starting index. Starting index changes when new line segment starts or whenevermodel_linear_trend_deviate_count  is greater than model_linear_trend_run_threshold     
  #'   
  #'   last_valid_line_start_timestamp stores last valid line segment start stamp
  #'   
  #'   last_valid_line_end_timestamp stores last valid line segment end stamp
  #'   
  #'   line_segment_start_index stores spatial index of current line segment start point in the given cycle data
  #'  
  #'  Return values:
  #'  
  #'    intercept - Intercept of current valid line segment. This value is same as last_valid_line_intercept, if there is not enough data for fitting new line segment
  #'    
  #'    slope - Slope of current valid line segment. This value is same as last_valid_line_slope, if there is not enough data for fitting new line segment
  #'    
  #'    valid_line_segment_index -  Gives the valid line segment index
  #'    
  #'    valid_line_start_timestamp      valid line start timestamp 
  #'    
  #'    valid_line_end_timestamp valid line end timestamp 
  #'    
  #'    line_segment_start_timestamp start time stamp of data taken for current line segment fitting
  #'    
  #'    line_segment_end_timestamp end time stamp of data taken for current line segment fitting
  #'    
  #'    filtered_nabt_value - storage for NABT values after checking the filtering conditions based on process parameters
  #'    
  #'    outlier_removed_nabt - storage for NABT values after checking for outlier condition
  #'    
  #' @param parameters We need some of the predefined variables from parameters for checkLinearity code - fitersize,minimumNoOfDaysDataForModel,model_eigen_threshold    and model_linear_trend_deviate_count 
  #' @param input_data inputdata is cycle data up to currentIndex (from fist day to current time stamp)
  #'
  #' @return vector of values 
  PWLSFit <- function(parameters,input_data) {
    
    slope <- NaN
    intercept <- NaN
    smoothed_nabt<-NaN
    all_slopes<-NaN
    all_intercepts<-NaN
    
    #start fitting line when minumum number of points reach parameters$cycle_days_burnin
    if((dim(input_data)[1])<=parameters$cycle_days_burnin)
    {
      line_segment_start_index <- parameters$cycle_days_burnin
    }else{
      line_segment_start_index<- which(input_data$Date == parameters$last_valid_line_end_timestamp)
    }

    # when input data length is less than line_segment_start_index, we don't do any analysis
    if(dim(input_data)[1]>line_segment_start_index)
    {
      #initially current line number is zero for the data when input data length is less than line_segment_start_index
      #when input data length greater than line_segment_start_index and if current_line_number is zero then we need to start first line segment.
      #So update current line number to one
      if(parameters$current_line_number==0)
      {
        parameters$current_line_number = 1
      }
      
      
      #storage for NABT value after checking the filtering conditions based on process parameters
      filtered_nabt_value                                   <- input_data$NABT[dim(input_data)[1]]
      fit_val                                               <- checkLinearity(parameters,input_data$NABT,line_segment_start_index)
      intercept                                             <- fit_val[['intercept']]
      slope                                                 <- fit_val[['slope']]
      parameters$model_linear_trend_deviate_count           <- fit_val[['model_linear_trend_deviate_count']]
      parameters$last_valid_line_slope                      <- fit_val[['last_valid_line_slope']]
      parameters$last_valid_line_intercept                  <- fit_val[['last_valid_line_intercept']]
      parameters$current_line_number                        <- fit_val[['current_line_number']]
      parameters$line_computed_for_model_regression_min_run <- fit_val[['line_computed_for_model_regression_min_run']]
      #store the NABT value after checking for outlier condition
      outlier_removed_nabt                                  <- fit_val[['outlier_removed_nabt']]
      smoothed_nabt                                         <- fit_val[['smoothed_nabt']]
      all_slopes                                            <- fit_val[['all_slopes']]
      all_intercepts                                        <- fit_val[['all_intercepts']]
      
      
      line_segment_start_timestamp <- as.character(input_data$Date[line_segment_start_index])
      line_segment_end_timestamp   <- as.character(input_data$Date[dim(input_data)[1]])
      
      #if slope is NaN, then data is not sufficient for line fitting. In this case we don't have enough information for projection
      
      if(is.nan(as.numeric(slope)) || slope <0) {
        valid_line_segment_index   <- NaN
        valid_line_start_timestamp <- NaN
        valid_line_end_timestamp   <- NaN
        slope                      <- NaN
        intercept                  <- NaN
        x_start                    <- NaN
        y_start                    <- NaN
        x_end                      <- NaN
        y_end                      <- NaN
      }else{
        valid_line_start_timestamp   <- as.character(input_data$Date[line_segment_start_index])
        valid_line_end_timestamp     <- as.character(input_data$Date[dim(input_data)[1]])
        if(parameters$line_computed_for_model_regression_min_run==-1)
        {
          valid_line_segment_index     <- NaN
        }else{
          valid_line_segment_index     <- parameters$current_line_number
        }
        x_start                      <- valid_line_start_timestamp
        x_end                        <- valid_line_end_timestamp
        y_start                      <- (slope*line_segment_start_index)+intercept
        y_end                        <- slope*(dim(input_data)[1])+intercept
      }
      
      if (parameters$model_linear_trend_deviate_count > parameters$model_linear_trend_run_threshold) {
        parameters$last_valid_line_start_timestamp            <- as.character(input_data$Date[line_segment_start_index])
        parameters$last_valid_line_end_timestamp              <- as.character(input_data$Date[dim(input_data)[1]])
        parameters$model_linear_trend_deviate_count           <- 0
        parameters$current_line_number                        <- parameters$current_line_number +1
        parameters$line_computed_for_model_regression_min_run <- 0
        x_start                                               <- as.character(input_data$Date[dim(input_data)[1]])                        
      }
    }else{
      # when input data length is less than line_segment_start_index, we don't do any analysis. So all the output values should be NaNs
      line_segment_start_timestamp <- NULL
      parameters$last_valid_line_end_timestamp <- as.Date(as_datetime(input_data$Date[1])) %m+% days(parameters$cycle_days_burnin)
      line_segment_end_timestamp   <- NaN
      valid_line_start_timestamp   <- NaN
      valid_line_end_timestamp     <- NaN
      valid_line_segment_index     <- NaN
      outlier_removed_nabt         <- NaN
      filtered_nabt_value          <- NaN
      x_start                    <- NaN
      y_start                    <- NaN
      x_end                      <- NaN
      y_end                      <- NaN
    }
    
    
    
    return(list(    "parameters"                   = parameters
                    , "intercept"                    = intercept
                    , "slope"                        = slope
                    , "valid_line_segment_index"     = valid_line_segment_index
                    , "valid_line_start_timestamp"   = valid_line_start_timestamp
                    , "valid_line_end_timestamp"     = valid_line_end_timestamp
                    , "line_segment_start_timestamp" = line_segment_start_timestamp
                    , "line_segment_end_timestamp"   = line_segment_end_timestamp
                    , "filtered_nabt_value"          = filtered_nabt_value
                    , "outlier_removed_nabt"         = outlier_removed_nabt
                    , "smoothed_nabt"                = smoothed_nabt
                    ,"all_slopes"                    = all_slopes
                    ,"all_intercepts"                = all_intercepts
                    ,"x_start"                       = x_start
                    ,"y_start"                       = y_start
                    ,"x_end"                         = x_end
                    ,"y_end"                         = y_end
    ))
  }
  
  
  
  
  # ----------------------------------------------------------------------------
  # Create return object throughout code
  # ----------------------------------------------------------------------------
  
  # Create new list to populate for return
  lst_ret <- list()  
  
  
  # ACAT/HDN split: do logic that is different for each of these cases
  if ( payload[['calc_type']] == "HDN" ) {
    
    
    # fresh_feed is Feed rate(RC basis)
    # fresh_nitrogen is Fresh Feed N content
    # NABT is Normalized T Cycle Avg
    name_rename <-
      c(   "fresh_feed"     = "FI_30005_01.PV_BVFCR"
           , "fresh_nitrogen" = "SN_30005_01_N_TOT.PV_RW"
           , "NABT"           = "HDNDAR_01.HDN_NABT_RC")
    
    #rename some of the paramters and copy the time series data in to dat_nabt
    dat_nabt <- payload$data_timeseries %>%
      rename_(.dots=name_rename)
    
    
    # HDN filter logic
    
    # Fresh Feed Filter
    # .................
    # Note: when this condition is true, we want to set NABT to NA.
    # NABT should not be used if the actual fresh feed rate (fresh_feed) is below the design percentage of the HDN fresh_feed design value.
    filter_fresh_feed <- dat_nabt$fresh_feed < (payload[['parameters']]$design_fresh_feed_percentage * payload[['parameters']]$design_fresh_feed_value)
    
    # filter_fresh_feed, alter the NABT in the data frame here.
    dat_nabt <- dat_nabt %>%
      mutate(NABT = ifelse(filter_fresh_feed, NA, NABT))
    
    
    
    # Nitrogen-feed filter
    # ....................
    # This condition is removing most of the data. We will not consider this condition
    # TODO: Mahesh.  Please confirm with Mike what is the correct filter here, or remove the line totally.
    # filter_fresh_nitrogen <- which((dat_nabt$fresh_nitrogen > (parameters$design_nitrogen_feed_value + parameters$design_nitrogen_feed_percentage_threshold )) | (dat_nabt$fresh_nitrogen < (parameters$design_nitrogen_feed_value - parameters$design_nitrogen_feed_percentage_threshold )))
    
    # filter_fresh_nitrogen
    #dat_nabt[filter_fresh_nitrogen,] <- NA
    
    
    
  } else if ( payload[['calc_type']] == "ACAT" ) {
    
    
    # fresh_feed is Feed rate(RC basis)
    # fresh_nitrogen is Fresh Feed N content
    # NABT is Normalized T Cycle Avg
    name_rename <-
      c(    "fresh_feed_1"     = "FI_30202_01.PV_BVFCR"
            ,"fresh_feed_2"    = "FI_30212_01.PV_BVFCR"
            ,"net_conversion"  = "E301351_01.NET_CONV_RC"
            ,"NABT"            = "ACATDAR_01.ACAT_NABT_RC")
    
    #rename some of the paramters and copy the time series data in to dat_nabt
    dat_nabt <- payload$data_timeseries %>%
      rename_(.dots=name_rename)
    
    # ACAT filter logic.
    # Fresh Feed Filter
    # .................
    # Note: when this condition is true, we want to set NABT to NA.
    # NABT should not be used if the actual fresh feed rate (FF)
    #   is below the design percentage of the ACAT FF design value.
    
    # dat_nabt$NABT = as.numeric(dat_nabt$NABT)
    # dat_nabt$fresh_feed <- as.numeric(dat_nabt$fresh_feed_1)+as.numeric(dat_nabt$fresh_feed_2)
    dat_nabt$fresh_feed <- dat_nabt$fresh_feed_1 + dat_nabt$fresh_feed_2
    
    
    filter_fresh_feed <- dat_nabt$fresh_feed < (payload[['parameters']]$design_fresh_feed_percentage * payload[['parameters']]$design_fresh_feed_value* payload[['parameters']]$design_cfr)
    
    # Inspect what I've set as filter
    
    dat_nabt <- dat_nabt%>%
      mutate(NABT = ifelse(filter_fresh_feed, NA, NABT))
    
    # Net Conversion filter
    # .................
    # Note: when this condition is true, we want to set NABT to NA.
    # NABT should not be used if the actual Net Conversion
    #   is below the design percentage of the ACAT Net Conversion design value.
    
    filter_net_conversion <- dat_nabt$net_conversion < (payload[['parameters']]$design_net_conversion_percentage * payload[['parameters']]$design_net_conversion_value)
    
    dat_nabt <- dat_nabt %>%
      mutate(NABT = ifelse(filter_net_conversion, NA, NABT))
    
    
  } else {
    print("Fill in error handling here, unsupported calc type.")
  }
  
  
  
  # After filtering (adding NAs where necessary), we do piecewise linear fit to given batch data.
  outputData <- PWLSFit(payload[['parameters']],dat_nabt)
  
  
  
  
  
  # ============================================================================
  # Assemble output
  # ============================================================================
  # Example of current output format -------------------------------------------
  # NOTE: filter_name1_applied_at is a placeholder - we need to determine the real filter names.
  
  lst_ret <- list(
    model_version                = ..version..
    , run_timestamp                = Sys.time()
    , calc_type                    = payload[['calc_type']]
    , parameters                   = outputData[['parameters']]
    , intercept                    = outputData[['intercept']]
    , slope                        = outputData[['slope']]
    , valid_line_segment_index     = outputData[['valid_line_segment_index']]
    , valid_line_start_timestamp   = outputData[['valid_line_start_timestamp']]
    , valid_line_end_timestamp     = outputData[['valid_line_end_timestamp']]
    , line_segment_start_timestamp = outputData[['line_segment_start_timestamp']]
    , line_segment_end_timestamp   = outputData[['line_segment_end_timestamp']]
    , filtered_nabt_value          = outputData[['filtered_nabt_value']]
    , outlier_removed_nabt         = outputData[['outlier_removed_nabt']]
    , smoothed_nabt                = outputData[['smoothed_nabt']]
    , all_slopes                   = outputData[['all_slopes']]
    , all_intercepts               = outputData[['all_intercepts']]
    , cycle_date_start             = min(dat_nabt$Date)
    , cycle_date_current           = max(dat_nabt$Date)
    , cycle_date_ndays             = length(dat_nabt$Date)
    , y_start                      = outputData[['y_start']]
    , y_end                        = outputData[['y_end']]
  )
  
  
  # ----------------------------------------------------------------------------
  # For WABT on data_timeseries on input ([HDN|ACAT]DAR_01.TEMP_WABT_RC), take average
  # of last 5 known values, and return.
  # ----------------------------------------------------------------------------
  # Get the wabt column.  Dynamic, as they are different between HDN and ACAT calls.
  dfcol_wabt <- names(dat_nabt) %>% str_subset(".*WABT_RC$")
  if (length(dfcol_wabt) > 0) {
    wabt_input <- dat_nabt[[dfcol_wabt]]
  } else {
    wabt_input <- NA_real_
  }
  
  # vector of wabt values, NAs removed.
  wabt_input_nomiss <- wabt_input[!is.na(wabt_input)]
  
  # How many of the most recent obs should we be averaging?
  wabt_obs_length <- 5
  
  # condition: Take obs of min of wabt_obs_length or however many good obs there are.
  wabt_take_obs_n <- pmin(wabt_obs_length, length(wabt_input_nomiss))
  
  # Compute mean of wabt tail.
  wabt_avg <- NA_real_
  if ( wabt_take_obs_n > 0) {
    wabt_avg <- mean(tail(wabt_input_nomiss, wabt_take_obs_n))
  }
  
  # Put this value on the output list
  lst_ret['wabt_avg'] <- wabt_avg
  
  
  
  
  # ----------------------------------------------------------------------------
  # Average non-wabt columns over last range of valid linear fit to data.
  # ----------------------------------------------------------------------------
  
  # Get the original dataframe
  dat_segmeans <- payload$data_timeseries
  
  if ( payload[['calc_type']] == "ACAT" ) {
    # Combine Fresh Feed
    # ..................................
    # https://acsjira.honeywell.com/browse/CTPDS-240
    # Need to combine "FI_30202_01.PV_BVFCR" and "FI_30212_01.PV_BVFCR" into single, summed flow.
    dat_segmeans <- dat_segmeans %>%
      mutate(ACATDAR_01.STLIQVOLFL_AVE_DAR = FI_30202_01.PV_BVFCR + FI_30212_01.PV_BVFCR)
  }
  
  # Get cols we want to deal with
  # The str_subset list are tag suffixes to exclude
  v_dfcol_keep <- names(dat_segmeans) %>%
    str_subset(".*(WABT_RC|NABT_RC|DOS_RC)$") %>%
    c("Date") %>%
    {setdiff(names(dat_segmeans), .)}
  # v_dfcol_keep %>% print()
  
  # Initialize all of the mean values of the columns of interest to NAs.
  v_segmeans <- rep(NA_real_, length(v_dfcol_keep))
  names(v_segmeans) <- v_dfcol_keep
  # v_segmeans %>% print()
  
  # Parse the dates 
  # Will be NA if they don't parse.
  valid_start_time <- lst_ret$valid_line_start_timestamp %>% parse_date_time("Y-m-d", quiet = TRUE)
  valid_end_time <- lst_ret$valid_line_end_timestamp %>% parse_date_time("Y-m-d", quiet = TRUE)
  # str(valid_start_time)
  # str(valid_end_time)
  
  # If we have the valid dates, proceed to computations
  if(!is.na(valid_start_time) && !is.na(valid_end_time) && length(v_dfcol_keep) > 0) {
    # Convert POSIXct to Date
    valid_start_time <- valid_start_time %>% as_date()
    valid_end_time <- valid_end_time %>% as_date()
    
    # dat_segmeans %>%
    #   mutate(Date = as_date(Date)) %>%
    #   filter(between(Date, valid_start_time, valid_end_time)) %>% glimpse()
    
    # Get the right columns and filter to be between right dates
    v_segmeans <- dat_segmeans %>%
      mutate(Date = as_date(Date)) %>%
      filter(between(Date, valid_start_time, valid_end_time)) %>%
      select_(.dots=v_dfcol_keep) %>%
      colMeans(na.rm = TRUE)
    # v_segmeans %>% str()
    # v_segmeans["FI_30005_01.PV_BVFCR"] %>% print()
  } else {
    
    print("a date is unparseable.")
  }
  
  # Add the computed values to the lst_ret
  for (.nm in names(v_segmeans)) {
    lst_ret[[sprintf("%s.SEGMEAN", .nm)]] <- v_segmeans[.nm]
  }
  .nm <- NULL
  
  
  
  # ----------------------------------------------------------------------------
  # Convert dates to epoch times
  # ----------------------------------------------------------------------------
  
  # Top-level names
  for (.nm in c("line_segment_start_timestamp"
                , "line_segment_end_timestamp"
                , "valid_line_start_timestamp"
                , "valid_line_end_timestamp"
                , "cycle_date_start"
                , "cycle_date_current")) {
    
    if(!is.na(lst_ret[[.nm]]) && !is.null(lst_ret[[.nm]])) {
      lst_ret[[.nm]] <- lst_ret[[.nm]] %>% ymd() %>% as_datetime() %>% as.numeric()
      
    }
  }

  # Those nested inside `parameters`
  for (.nm in c("last_valid_line_end_timestamp"
                , "last_valid_line_start_timestamp")) {
    
    if(!is.na(lst_ret[["parameters"]][[.nm]]) && !is.null(lst_ret[["parameters"]][[.nm]])) {
      
      lst_ret[["parameters"]][[.nm]] <- lst_ret[["parameters"]][[.nm]] %>%  ymd() %>% as_datetime() %>% as.numeric()
    }
  }

  
  
  # ----------------------------------------------------------------------------
  # https://acsjira.honeywell.com/browse/CPSRA-646
  # Add field cycle_end_date_proj for projected endpoint of bed life based on
  #   wabt values.
  # ----------------------------------------------------------------------------
  #browser()
  lst_ret[["cycle_end_date_proj"]] <- (lst_ret[["parameters"]][["NABTDesign"]] - lst_ret[["wabt_avg"]]) * 86400 / lst_ret[["parameters"]][["last_valid_line_slope"]] + lst_ret[["cycle_date_current"]]
  lst_ret[["cycle_end_date_proj"]] <- lst_ret[["cycle_end_date_proj"]] %>% round()
  
  # ============================================================================
  # Return output --------------------------------------------------------------
  # ============================================================================
  
  # ...
  # pre-processing
  # ...
  
  # if (nrow(dat_nabt) >= 200) str(lst_ret)
  
  # Top-level objects with empty lists set to null values.
  lst_ret <- lst_ret %>% map_if(is_empty, ~ NULL)
  
  # browser()
  
  # 2nd level parameter objects with empty lists set to null values.
  # lst_ret$parameters <- lst_ret$parameters %>% map_if(is_empty, ~ NULL)
  # lst_ret$parameters <- lst_ret$parameters %>% map_if(~ !is.null(.x) && .x == " ", ~ NULL)
  
  for (.nm in lst_ret[["parameters"]] %>% names()) {
    # cat("\n--- ", .nm, "\n")
    # print(lst_ret[["parameters"]][[.nm]])
    # print(class(lst_ret[["parameters"]][[.nm]]))
   
    # https://gist.github.com/mpettis/a780bed132a5b13d2ceceb30546d1700
    
    if (is.null(lst_ret[["parameters"]][[.nm]])) {
      # cat("--> is.na", "\n")
      lst_ret[["parameters"]][.nm] <- list(NULL)
      next
    }
    
    if (is.na(lst_ret[["parameters"]][[.nm]])) {
      # cat("--> is.na", "\n")
      lst_ret[["parameters"]][.nm] <- list(NULL)
      next
    }
    
    if (lst_ret[["parameters"]][[.nm]] == " ") {
      # cat("--> is blank", "\n")
      lst_ret[["parameters"]][.nm] <- list(NULL)
      next
    }
    # cat("\n")
  }
  
  
  # Create return payload of encoded JSON
  payload_ret <- lst_ret %>%
    toJSON(auto_unbox=TRUE, null="null", na = "null") %>%
    base64_enc()
  
  return(payload_ret)
  #return(paste0(capture.output(sessionInfo()), collapse="\n"))
  #return(paste0(capture.output(installed.packages()), collapse="\n"))
}
