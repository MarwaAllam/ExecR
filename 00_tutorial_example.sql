SELECT * FROM TD_SYSGPL.ExecR (
  ON (SELECT * FROM twm_customer_analysis_train) 
    PARTITION BY ANY     -- data and partitioning
  USING
     -- to define output schema
    Contract(' 
      library(tdr)
      streamno_out <- 0
      coltype_varchar <- list(datatype="VARCHAR_DT", 
                              charset="LATIN_CT", 
                              size.length=200)
      coltype_real    <- list(datatype="REAL_DT", 
                              bytesize="SIZEOF_REAL")
      coltype_integer <- list(datatype="INTEGER_DT", 
                              bytesize="SIZEOF_INTEGER")
      coldef <- list(
        id =        coltype_varchar, 
        mean_age =  coltype_real,
        row_cnt =   coltype_integer )
      tdr.SetOutputColDef(streamno_out, coldef)
    ')
    -- R script for processing
    Operator('
      library(tdr)
    
      direction_in <- "R"
      streamno_in <- 0
      options <- 0
      handle_in <- tdr.Open(direction_in, streamno_in, options)

      dat_export <- data.frame(
        id =  paste(sort(unique(dat$marital_status)), collapse=", "),
        mean_age = mean(dat$age, na.rm = TRUE),
        row_cnt = nrow(dat),
        stringsAsFactors = FALSE
      )
      
      direction_out <- "W"
      streamno_out <- 0
      handle_out <- tdr.Open(direction_out, streamno_out, options)
      tdr.TblWrite(handle_out, dat_export)
      tdr.Close(handle_out)
    ')
    keeplog(0)           -- options
) as queryname;          -- query needs to be named
