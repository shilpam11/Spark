
rm(list = ls())
library(SparkR)
library(sparklyr)
library(dplyr)
library(ggplot2)

# Check envirnment variables
Sys.getenv()

# Configure cluster
Sys.setenv(SPARK_HOME="/usr/lib/spark")
config <- spark_config()
config$spark.driver.cores   <- 2
config$spark.executor.cores <- 2
config$spark.executor.memory <- "4g"
# Connect to cluster
sc <- spark_connect(master = "yarn-client", config = config, version = '1.6.1')

spark_connection_is_open(sc)

help("spark_read_csv")
sparklyr::spark_read_csv(sc, 'sdf_2015', "s3://satyesh-spark-assgn/ParkingData/2015.csv", header = T, infer_schema = T, memory = T)
sparklyr::spark_read_csv(sc, 'sdf_2016', "s3://satyesh-spark-assgn/ParkingData/2016.csv", header = T, infer_schema = T, memory = T)
sparklyr::spark_read_csv(sc, 'sdf_2017', "s3://satyesh-spark-assgn/ParkingData/2017.csv", header = T, infer_schema = T, memory = T)
sparklyr::spark_read_csv(sc, 'sdf_codes', "s3://satyesh-spark-assgn/ParkingData/codes.csv", header = T, infer_schema = T, memory = T)

#sdf_2015 <- SparkR::read.df("s3://shilpa-spark-assgn/ParkingData/2015.csv", header=T, "CSV")
#sdf_2016 <- SparkR::read.df("s3://shilpa-spark-assgn/ParkingData/2016.csv", header=T, "CSV")
#sdf_2017 <- SparkR::read.df("s3://shilpa-spark-assgn/ParkingData/2017.csv", header=T, "CSV")
#sdf_codes <- SparkR::read.df("s3://shilpa-spark-assgn/ParkingData/codes.csv", header=T, "CSV")

sdf_2015 <- tbl(sc, 'sdf_2015')
sdf_2016 <- tbl(sc, 'sdf_2016')
sdf_2017 <- tbl(sc, 'sdf_2017')
sdf_codes <- tbl(sc, 'sdf_codes')


#Check the record count
sdf_2015 %>% count  #11809233
sdf_2016 %>% count  #10626899
sdf_2017 %>% count  #10803028


#Check for duplicates
sdf_2015 %>% group_by(Summons_Number) %>% summarize(rec_count = n()) %>% filter(rec_count > 1) %>% count 
#809112
sdf_2016 %>% group_by(Summons_Number) %>% summarize(rec_count = n()) %>% filter(rec_count > 1) %>% count 
#0
sdf_2017 %>% group_by(Summons_Number) %>% summarize(rec_count = n()) %>% filter(rec_count > 1) %>% count 
#0


#Chk nulls in Summons_Number
sdf_2015 %>% filter(is.na(Summons_Number)) %>% count #no nulls
sdf_2016 %>% filter(is.na(Summons_Number)) %>% count #no nulls
sdf_2017 %>% filter(is.na(Summons_Number)) %>% count #no nulls


#remove duplicates from 2015 data
sdf_2015_dedup <- sdf_2015 %>% group_by(Summons_Number, Vehicle_Body_Type, Vehicle_Make, House_Number, Street_Name, Violation_Code, Registration_State, Issue_Date, Violation_Time, Violation_Precinct, Issuer_Precinct) %>% summarize(rec_cnt = n()) %>% sdf_register("sdf_2015_dedup")
tbl_cache(sc, "sdf_2015_dedup")
sc %>% spark_session() %>% invoke("catalog") %>% invoke("dropTempView", "sdf_2015")
sdf_2015_dedup %>% count  #10951256

src_tbls(sc)


#Add File Year column to the data tbl_spark
sdf_2015_dedup_fyr <- sdf_2015_dedup %>% mutate(File_Yr = '2015') %>% sdf_register('sdf_2015_dedup_fyr')
tbl_cache(sc, "sdf_2015_dedup_fyr")
sdf_2015_dedup_fyr %>% select(Summons_Number, File_Yr, Issue_Date) #, Issue_Day, Issue_Year, Issue_Month, Violation_Time, VT_hour, VT_AMPM) %>% head
sc %>% spark_session() %>% invoke("catalog") %>% invoke("dropTempView", "sdf_2015_dedup")


#Add File Year column to the 2016 data tbl_spark
sdf_2016_fyr <- sdf_2016 %>% mutate(File_Yr = '2016') %>% sdf_register('sdf_2016_fyr')
tbl_cache(sc, "sdf_2016_fyr")
sdf_2016_fyr %>% select(Summons_Number, File_Yr, Issue_Date) #, Issue_Day, Issue_Year, Issue_Month, Violation_Time, VT_hour, VT_AMPM) %>% head
sc %>% spark_session() %>% invoke("catalog") %>% invoke("dropTempView", "sdf_2016")



#Add File Year column to the 2017 data tbl_spark
sdf_2017_fyr <- sdf_2017 %>% mutate(File_Yr = '2017') %>% sdf_register('sdf_2017_fyr')
tbl_cache(sc, "sdf_2017_fyr")
sdf_2017_fyr %>% select(Summons_Number, File_Yr, Issue_Date) #, Issue_Day, Issue_Year, Issue_Month, Violation_Time, VT_hour, VT_AMPM) %>% head
sc %>% spark_session() %>% invoke("catalog") %>% invoke("dropTempView", "sdf_2017")



#Making a single table for all the files
sdf_ParkingData <- union((union( sdf_2015_dedup_fyr %>% select(Summons_Number, Vehicle_Body_Type, Vehicle_Make, House_Number, Street_Name, Violation_Code, Registration_State, Issue_Date, Violation_Time, Violation_Precinct, Issuer_Precinct, File_Yr),
                                 (sdf_2016_fyr %>% select(Summons_Number, Vehicle_Body_Type, Vehicle_Make, House_Number, Street_Name, Violation_Code, Registration_State, Issue_Date, Violation_Time, Violation_Precinct, Issuer_Precinct, File_Yr)))),
                         (sdf_2017_fyr %>% select(Summons_Number, Vehicle_Body_Type, Vehicle_Make, House_Number, Street_Name, Violation_Code, Registration_State, Issue_Date, Violation_Time, Violation_Precinct, Issuer_Precinct, File_Yr))) %>% sdf_register("sdf_ParkingData")

tbl_cache(sc, "sdf_ParkingData")

#Overall Count
sdf_ParkingData %>% count #32381183
sc %>% spark_session() %>% invoke("catalog") %>% invoke("dropTempView", "sdf_2015_dedup_fyr")
sc %>% spark_session() %>% invoke("catalog") %>% invoke("dropTempView", "sdf_2016_fyr")
sc %>% spark_session() %>% invoke("catalog") %>% invoke("dropTempView", "sdf_2017_fyr")


#Checking for duplicate summons in collective data
#sdf_ParkingData %>% group_by(Summons_Number) %>% summarise(rec_count = n()) %>% filter(rec_count > 1) %>% count # 224875 duplicate summons
#sdf_ParkingData_dedup <- sdf_ParkingData %>% group_by(Summons_Number, Vehicle_Body_Type, Vehicle_Make, House_Number, Street_Name, Violation_Code, Registration_State, Issue_Date, Violation_Time, Violation_Precinct, Issuer_Precinct, File_Yr) %>% summarise(rec_count = n()) %>% sdf_register('sdf_ParkingData_dedup')
#tbl_cache(sc, 'sdf_ParkingData_dedup')
#sdf_ParkingData_dedup %>% count #32381183
#Duplicate summons with different details are encountered in the data #224675 such records
#sc %>% spark_session() %>% invoke("catalog") %>% invoke("dropTempView", "sdf_parkingdata_dedup")


#Adding fields in data
sdf_ParkingData_new <- sdf_ParkingData %>% mutate(Issue_Date_2 = sql("CAST(CAST(unix_timestamp(Issue_Date, 'MM/dd/yyyy') AS timestamp) AS date)"),
                                                  Issue_Day = dayofmonth(Issue_Date_2),
                                                  Issue_Month = month(Issue_Date_2),
                                                  Issue_Year = year(Issue_Date_2),
                                                  VT_hour = as.integer(substr(Violation_Time,1, 2)),
                                                  VT_AMPM = substr(Violation_Time,5, 5)) %>% sdf_register('sdf_ParkingData_new')
tbl_cache(sc, 'sdf_ParkingData_new')
sdf_ParkingData_new %>% select(Summons_Number, File_Yr, Issue_Date, Issue_Date_2, Issue_Day, Issue_Year, Issue_Month, Violation_Time, VT_hour, VT_AMPM) %>% head
sdf_ParkingData_new %>% count #32381183
sc %>% spark_session() %>% invoke("catalog") %>% invoke("dropTempView", "sdf_parkingdata")
#spark_write_csv(sdf_vb_smry, 's3://satyesh-spark-assgn/ParkingData_Clean/ParkingData_Clean.csv')


#Checking for valid fiscal date range in the data
#year 2015
sdf_date_smry <- sdf_ParkingData_new %>% group_by(Issue_Date_2) %>% summarise(rec_count = n())
rdf_date_smry <- as.data.frame(sdf_date_smry)
View(rdf_date_smry)
#Initial analysis showed that Data in files for year 2015, 2016, 2017 contains summons from previous years also
#Year will be considered from 1-Oct-2014 to 30-Sep-2015 for year wise summaries


rdf_date_smry$FiscalYr <- 'OtherYr'
rdf_date_smry$FiscalYr[which(rdf_date_smry$Issue_Date_2 >= '2014-10-01' & rdf_date_smry$Issue_Date_2 <= '2015-09-30')] <- 'FY2015'
rdf_date_smry$FiscalYr[which(rdf_date_smry$Issue_Date_2 >= '2015-10-01' & rdf_date_smry$Issue_Date_2 <= '2016-09-30')] <- 'FY2016'
rdf_date_smry$FiscalYr[which(rdf_date_smry$Issue_Date_2 >= '2016-10-01' & rdf_date_smry$Issue_Date_2 <= '2017-09-30')] <- 'FY2017'

#1.Year wise summons count
rdf_yearwise_summons <- rdf_date_smry %>% group_by(FiscalYr) %>% summarise(rec_count = sum(rec_count))
rdf_yearwise_summons

#FiscalYr rec_count
#1 FY2015   10761151.
#2 FY2016   10392298.
#3 FY2017    8080120.
#4 OtherYr   3147614.

ggplot(rdf_yearwise_summons, aes(x = FiscalYr, y = rec_count/1000000, fill = factor(FiscalYr))) + 
  geom_col(width = 0.5) + 
  xlab("Fiscal Year") +
  ylab("No. of Parking Tickets(In Mn)") +
  theme(axis.text.x = element_text(angle = 90), legend.position = 'none') +
  scale_x_discrete(limits = c('FY2015', 'FY2016', 'FY2017')) +
  ggtitle("Yearwise Parking Tickets") 

ggsave("Year Wise Parking.jpg")

#2. Unique States which got Parking Tickets
#Filter the data as per year on issue date and
#summarize as per date and registration state
sdf_state_smry <- sdf_ParkingData_new %>%   
  filter(Issue_Year == 2014 | Issue_Year == 2015 | Issue_Year == 2016 | Issue_Year == 2017) %>%
  select(Issue_Date_2, Registration_State) %>% 
  group_by(Issue_Date_2, Registration_State) %>% 
  summarise(st_count = n())
rdf_state_smry <- as.data.frame(sdf_state_smry)
rdf_state_smry %>% head()

#Assign the fiscal year as per the issue date
rdf_state_smry$FiscalYr <- 'OtherYr'
rdf_state_smry$FiscalYr[which(rdf_state_smry$Issue_Date_2 >= '2014-10-01' & rdf_state_smry$Issue_Date_2 <= '2015-09-30')] <- 'FY2015'
rdf_state_smry$FiscalYr[which(rdf_state_smry$Issue_Date_2 >= '2015-10-01' & rdf_state_smry$Issue_Date_2 <= '2016-09-30')] <- 'FY2016'
rdf_state_smry$FiscalYr[which(rdf_state_smry$Issue_Date_2 >= '2016-10-01' & rdf_state_smry$Issue_Date_2 <= '2017-09-30')] <- 'FY2017'
rdf_state_smry %>% head()

rdf_states <- rdf_state_smry %>% 
  filter(FiscalYr != 'OtherYr') %>%
  group_by(Registration_State) %>% 
  summarise(rec_count = sum(st_count))
nrow(rdf_states)
#69 states


#Number of Parking Tickets with no addresses
sdf_noaddr_smry <- sdf_ParkingData_new %>%   
  filter(Issue_Year == 2014 | Issue_Year == 2015 | Issue_Year == 2016 | Issue_Year == 2017) %>%
  filter(isNull(House_Number) | isNull(Street_Name)) %>%
  select(Issue_Date_2) %>% 
  group_by(Issue_Date_2) %>% 
  summarise(rec_count = n())
rdf_noaddr_smry <- as.data.frame(sdf_noaddr_smry)
rdf_noaddr_smry %>% head()

rdf_noaddr_smry$FiscalYr <- 'OtherYr'
rdf_noaddr_smry$FiscalYr[which(rdf_noaddr_smry$Issue_Date_2 >= '2014-10-01' & rdf_noaddr_smry$Issue_Date_2 <= '2015-09-30')] <- 'FY2015'
rdf_noaddr_smry$FiscalYr[which(rdf_noaddr_smry$Issue_Date_2 >= '2015-10-01' & rdf_noaddr_smry$Issue_Date_2 <= '2016-09-30')] <- 'FY2016'
rdf_noaddr_smry$FiscalYr[which(rdf_noaddr_smry$Issue_Date_2 >= '2016-10-01' & rdf_noaddr_smry$Issue_Date_2 <= '2017-09-30')] <- 'FY2017'
rdf_noaddr_smry %>% head()

rdf_noaddrs <- rdf_noaddr_smry %>% 
  filter(FiscalYr != 'OtherYr') %>% 
  group_by(FiscalYr) %>% 
  summarise(rec_count = sum(rec_count))
rdf_noaddrs
sum(rdf_noaddrs$rec_count)
#5518976 summons with no address in the FY2015, FY2016, FY2017

#FiscalYr rec_count
#1 FY2015    1688076.
#2 FY2016    2135603.
#3 FY2017    1695297.


#Part 2 : Aggregation Tasks
#2.1. Frequency of Violation Code Top 5 File Year wise
sdf_vc_smry <- sdf_ParkingData_new %>%   
  filter(Issue_Year == 2014 | Issue_Year == 2015 | Issue_Year == 2016 | Issue_Year == 2017) %>%
  select(Issue_Date_2, Violation_Code) %>% 
  group_by(Issue_Date_2, Violation_Code) %>% 
  summarise(rec_count = n())
rdf_vc_smry <- as.data.frame(sdf_vc_smry)
rdf_vc_smry %>% head()

rdf_vc_smry$FiscalYr <- 'OtherYr'
rdf_vc_smry$FiscalYr[which(rdf_vc_smry$Issue_Date_2 >= '2014-10-01' & rdf_vc_smry$Issue_Date_2 <= '2015-09-30')] <- 'FY2015'
rdf_vc_smry$FiscalYr[which(rdf_vc_smry$Issue_Date_2 >= '2015-10-01' & rdf_vc_smry$Issue_Date_2 <= '2016-09-30')] <- 'FY2016'
rdf_vc_smry$FiscalYr[which(rdf_vc_smry$Issue_Date_2 >= '2016-10-01' & rdf_vc_smry$Issue_Date_2 <= '2017-09-30')] <- 'FY2017'
rdf_vc_smry %>% head()

rdf_vcs <- rdf_vc_smry %>% 
  group_by(FiscalYr, Violation_Code) %>% 
  summarise(rec_count = sum(rec_count))

get_yrwise_smry <- function(Yr, df, topcount){
  df1 <- df %>% filter(FiscalYr == Yr)
  df1_smry_sort <- order(df1$rec_count, decreasing = T, method = 'auto' )
  return(head(df1[df1_smry_sort,],topcount))
}

vc_top3_2015 <- get_yrwise_smry('FY2015', rdf_vcs, 3)
vc_top3_2016 <- get_yrwise_smry('FY2016', rdf_vcs, 3)
vc_top3_2017 <- get_yrwise_smry('FY2017', rdf_vcs, 3)

vc_top3_2015
vc_top3_2016
vc_top3_2017

rdf_top_vc <- rbind(vc_top3_2015, vc_top3_2016)
rdf_top_vc <- rbind(rdf_top_vc, vc_top3_2017)
rdf_top_vc$rec_count <- rdf_top_vc$rec_count / 1000000
rdf_top_vc$Violation_Code <- as.character(rdf_top_vc$Violation_Code)
str(rdf_top_vc)

ggplot(rdf_top_vc, aes(x = as.factor(Violation_Code), y = rec_count, fill = factor(FiscalYr))) + 
  geom_col(width = 0.5) + 
  facet_wrap(~FiscalYr, scale = 'free') +
  xlab("Violation Code") +
  ylab("No. of Parking Tickets (In Millions)") +
  theme(axis.text.x = element_text(angle = 90), legend.position = 'none') +
  #scale_x_discrete(limits = c('FY2015', 'FY2016', 'FY2017')) +
  ggtitle("Yearwise Parking Tickets - ") +
  labs(subtitle = 'Top 3 Violation Codes')

ggsave("Year Wise Violation Code Frequency.jpg")

#  FiscalYr Violation_Code rec_count
#1 FY2015               21  1508070.
#2 FY2015               38  1278718.
#3 FY2015               14   923869.

#  FiscalYr Violation_Code rec_count
#1 FY2016               21  1506924.
#2 FY2016               36  1344464.
#3 FY2016               38  1078318.

#  FiscalYr Violation_Code rec_count
#1 FY2017               21  1115585.
#2 FY2017               36  1105358.
#3 FY2017               38   805463.


#2.2.1 Top 5 Vehicle Body Type 
sdf_vb_smry <- sdf_ParkingData_new %>%   
  filter(Issue_Year == 2014 | Issue_Year == 2015 | Issue_Year == 2016 | Issue_Year == 2017) %>%
  select(Issue_Date_2, Vehicle_Body_Type) %>% 
  group_by(Issue_Date_2, Vehicle_Body_Type) %>% 
  summarise(rec_count = n())
rdf_vb_smry <- as.data.frame(sdf_vb_smry)
rdf_vb_smry %>% head()


rdf_vb_smry$FiscalYr <- 'OtherYr'
rdf_vb_smry$FiscalYr[which(rdf_vb_smry$Issue_Date_2 >= '2014-10-01' & rdf_vb_smry$Issue_Date_2 <= '2015-09-30')] <- 'FY2015'
rdf_vb_smry$FiscalYr[which(rdf_vb_smry$Issue_Date_2 >= '2015-10-01' & rdf_vb_smry$Issue_Date_2 <= '2016-09-30')] <- 'FY2016'
rdf_vb_smry$FiscalYr[which(rdf_vb_smry$Issue_Date_2 >= '2016-10-01' & rdf_vb_smry$Issue_Date_2 <= '2017-09-30')] <- 'FY2017'
rdf_vb_smry %>% head()

vb_type_list <- as.data.frame(unique(rdf_vb_smry$Vehicle_Body_Type)) 

rdf_vbs <- rdf_vb_smry %>% 
  group_by(FiscalYr, Vehicle_Body_Type) %>% 
  summarise(rec_count = sum(rec_count))

#rdf_vbs[which(rdf_vbs$Vehicle_Body_Type == 'VAN.'),]
#The Vehicle Body Type counts can be improved by cleaning the vehicle make information

vb_top3_2015 <- get_yrwise_smry('FY2015', rdf_vbs, 3)
vb_top3_2016 <- get_yrwise_smry('FY2016', rdf_vbs, 3)
vb_top3_2017 <- get_yrwise_smry('FY2017', rdf_vbs, 3)

vb_top3_2015
vb_top3_2016
vb_top3_2017

rdf_vb_top3 <- rbind(vb_top3_2015, vb_top3_2016)
rdf_vb_top3 <- rbind(rdf_vb_top3, vb_top3_2017)
rdf_vb_top3$rec_count <- rdf_vb_top3$rec_count / 1000000

ggplot(rdf_vb_top3, aes(x = as.factor(Vehicle_Body_Type), y = rec_count, fill = factor(FiscalYr))) + 
  geom_col(width = 0.5) + 
  facet_wrap(~FiscalYr, scale = 'free') +
  xlab("Vehicle Body Type") +
  ylab("No. of Parking Tickets (In Millions)") +
  theme(axis.text.x = element_text(angle = 90), legend.position = 'none') +
  #scale_x_discrete(limits = c('FY2015', 'FY2016', 'FY2017')) +
  ggtitle("Yearwise Parking Tickets - ") +
  labs(subtitle = 'Top 3 Vehicle Body Types')

ggsave("Year Wise Vehicle Body Type Frequency.jpg")


#  FiscalYr Vehicle_Body_Type rec_count
#1 FY2015   SUBN               3415605.
#2 FY2015   4DSD               3027901.
#3 FY2015   VAN                1582130.

#  FiscalYr Vehicle_Body_Type rec_count
#1 FY2016   SUBN               3444465.
#2 FY2016   4DSD               2937601.
#3 FY2016   VAN                1445801.

#  FiscalYr Vehicle_Body_Type rec_count
#1 FY2017   SUBN               2814791.
#2 FY2017   4DSD               2323842.
#3 FY2017   VAN                1041662.



#2.2.2 Top 5 Vehicle Make
sdf_vm_smry <- sdf_ParkingData_new %>%   
  filter(Issue_Year == 2014 | Issue_Year == 2015 | Issue_Year == 2016 | Issue_Year == 2017) %>%
  select(Issue_Date_2, Vehicle_Make) %>% 
  group_by(Issue_Date_2, Vehicle_Make) %>% 
  summarise(rec_count = n())
rdf_vm_smry <- as.data.frame(sdf_vm_smry)
rdf_vm_smry %>% head()


rdf_vm_smry$FiscalYr <- 'OtherYr'
rdf_vm_smry$FiscalYr[which(rdf_vm_smry$Issue_Date_2 >= '2014-10-01' & rdf_vm_smry$Issue_Date_2 <= '2015-09-30')] <- 'FY2015'
rdf_vm_smry$FiscalYr[which(rdf_vm_smry$Issue_Date_2 >= '2015-10-01' & rdf_vm_smry$Issue_Date_2 <= '2016-09-30')] <- 'FY2016'
rdf_vm_smry$FiscalYr[which(rdf_vm_smry$Issue_Date_2 >= '2016-10-01' & rdf_vm_smry$Issue_Date_2 <= '2017-09-30')] <- 'FY2017'
rdf_vm_smry %>% head()

vm_type_list <- as.data.frame(unique(rdf_vm_smry$Vehicle_Make)) 
#The Vehicle make counts can be improved by cleaning the vehicle make information

rdf_vms <- rdf_vm_smry %>% 
  group_by(FiscalYr, Vehicle_Make) %>% 
  summarise(rec_count = sum(rec_count))

vm_top3_2015 <- get_yrwise_smry('FY2015', rdf_vms, 3)
vm_top3_2016 <- get_yrwise_smry('FY2016', rdf_vms, 3)
vm_top3_2017 <- get_yrwise_smry('FY2017', rdf_vms, 3)

rdf_vms[which(rdf_vms$Vehicle_Make == 'HONDY'),]

vm_top3_2015
vm_top3_2016
vm_top3_2017

rdf_vm_top3 <- rbind(vm_top3_2015, vm_top3_2016)
rdf_vm_top3 <- rbind(rdf_vm_top3, vm_top3_2017)
rdf_vm_top3$rec_count <- rdf_vm_top3$rec_count / 1000000

ggplot(rdf_vm_top3, aes(x = as.factor(Vehicle_Make), y = rec_count, fill = factor(FiscalYr))) + 
  geom_col(width = 0.5) + 
  facet_wrap(~FiscalYr, scale = 'free') +
  xlab("Vehicle Make") +
  ylab("No. of Parking Tickets (In Millions)") +
  theme(axis.text.x = element_text(angle = 90), legend.position = 'none') +
  #scale_x_discrete(limits = c('FY2015', 'FY2016', 'FY2017')) +
  ggtitle("Yearwise Parking Tickets - ") +
  labs(subtitle = 'Top 3 Vehicle Makes')

ggsave("Year Wise Vehicle Make Frequency.jpg")


#  FiscalYr Vehicle_Make rec_count
#1 FY2015   FORD          1377444.
#2 FY2015   TOYOT         1113846.
#3 FY2015   HONDA         1003517.

#  FiscalYr Vehicle_Make rec_count
#1 FY2016   FORD          1279043.
#2 FY2016   TOYOT         1145346.
#3 FY2016   HONDA         1007007.

#  FiscalYr Vehicle_Make rec_count
#1 FY2017   FORD           946351.
#2 FY2017   TOYOT          911247.
#3 FY2017   HONDA          810702.


#2.3.1 Top 5 Violating Precincts
#Violating Precinct 0 is not considered for the analysis
sdf_vp_smry <- sdf_ParkingData_new %>%   
  filter(Violation_Precinct != 0) %>%
  filter(Issue_Year == 2014 | Issue_Year == 2015 | Issue_Year == 2016 | Issue_Year == 2017) %>%
  select(Issue_Date_2, Violation_Precinct) %>% 
  group_by(Issue_Date_2, Violation_Precinct) %>% 
  summarise(rec_count = n())
rdf_vp_smry <- as.data.frame(sdf_vp_smry)
rdf_vp_smry %>% head()


rdf_vp_smry$FiscalYr <- 'OtherYr'
rdf_vp_smry$FiscalYr[which(rdf_vp_smry$Issue_Date_2 >= '2014-10-01' & rdf_vp_smry$Issue_Date_2 <= '2015-09-30')] <- 'FY2015'
rdf_vp_smry$FiscalYr[which(rdf_vp_smry$Issue_Date_2 >= '2015-10-01' & rdf_vp_smry$Issue_Date_2 <= '2016-09-30')] <- 'FY2016'
rdf_vp_smry$FiscalYr[which(rdf_vp_smry$Issue_Date_2 >= '2016-10-01' & rdf_vp_smry$Issue_Date_2 <= '2017-09-30')] <- 'FY2017'
rdf_vp_smry %>% head()

vp_type_list <- as.data.frame(unique(rdf_vp_smry$Violation_Precinct)) 

rdf_vps <- rdf_vp_smry %>% 
  group_by(FiscalYr, Violation_Precinct) %>% 
  summarise(rec_count = sum(rec_count))


vp_top3_2015 <- get_yrwise_smry('FY2015', rdf_vps, 3)
vp_top3_2016 <- get_yrwise_smry('FY2016', rdf_vps, 3)
vp_top3_2017 <- get_yrwise_smry('FY2017', rdf_vps, 3)


vp_top3_2015
vp_top3_2016
vp_top3_2017

rdf_vp_top3 <- rbind(vp_top3_2015, vp_top3_2016)
rdf_vp_top3 <- rbind(rdf_vp_top3, vp_top3_2017)
rdf_vp_top3$rec_count <- rdf_vp_top3$rec_count / 1000

ggplot(rdf_vp_top3, aes(x = as.factor(Violation_Precinct), y = rec_count, fill = factor(FiscalYr))) + 
  geom_col(width = 0.5) + 
  facet_wrap(~FiscalYr, scale = 'free') +
  xlab("Violation Precinct") +
  ylab("No. of Parking Tickets (In Thousands)") +
  theme(axis.text.x = element_text(angle = 90), legend.position = 'none') +
  #scale_x_discrete(limits = c('FY2015', 'FY2016', 'FY2017')) +
  ggtitle("Yearwise Parking Tickets - ") +
  labs(subtitle = 'Top 3 Violation Precincts')

ggsave("Year Wise Violation Precinct Frequency.jpg")

#  FiscalYr Violation_Precinct rec_count
#1 FY2015                   19   567874.
#2 FY2015                   18   380261.
#3 FY2015                   14   372810.

#  FiscalYr Violation_Precinct rec_count
#1 FY2016                   19   537111.
#2 FY2016                   18   306044.
#3 FY2016                   14   302426.

#  FiscalYr Violation_Precinct rec_count
#1 FY2017                   19   394295.
#2 FY2017                   14   275231.
#3 FY2017                    1   252632.



#2.3.2 Top 5 Issuer Precincts
#Issuer Precinct 0 is not considered for analysis
sdf_ip_smry <- sdf_ParkingData_new %>%   
  filter(Issuer_Precinct != 0) %>%
  filter(Issue_Year == 2014 | Issue_Year == 2015 | Issue_Year == 2016 | Issue_Year == 2017) %>%
  select(Issue_Date_2, Issuer_Precinct) %>% 
  group_by(Issue_Date_2, Issuer_Precinct) %>% 
  summarise(rec_count = n())
rdf_ip_smry <- as.data.frame(sdf_ip_smry)
rdf_ip_smry %>% head()


rdf_ip_smry$FiscalYr <- 'OtherYr'
rdf_ip_smry$FiscalYr[which(rdf_ip_smry$Issue_Date_2 >= '2014-10-01' & rdf_ip_smry$Issue_Date_2 <= '2015-09-30')] <- 'FY2015'
rdf_ip_smry$FiscalYr[which(rdf_ip_smry$Issue_Date_2 >= '2015-10-01' & rdf_ip_smry$Issue_Date_2 <= '2016-09-30')] <- 'FY2016'
rdf_ip_smry$FiscalYr[which(rdf_ip_smry$Issue_Date_2 >= '2016-10-01' & rdf_ip_smry$Issue_Date_2 <= '2017-09-30')] <- 'FY2017'
rdf_ip_smry %>% head()

ip_type_list <- as.data.frame(unique(rdf_ip_smry$Issuer_Precinct)) 

rdf_ips <- rdf_ip_smry %>% 
  group_by(FiscalYr, Issuer_Precinct) %>% 
  summarise(rec_count = sum(rec_count))

ip_top3_2015 <- get_yrwise_smry('FY2015', rdf_ips, 3)
ip_top3_2016 <- get_yrwise_smry('FY2016', rdf_ips, 3)
ip_top3_2017 <- get_yrwise_smry('FY2017', rdf_ips, 3)


ip_top3_2015
ip_top3_2016
ip_top3_2017

rdf_ip_top3 <- rbind(ip_top3_2015, ip_top3_2016)
rdf_ip_top3 <- rbind(rdf_ip_top3, ip_top3_2017)
rdf_ip_top3$rec_count <- rdf_ip_top3$rec_count / 1000

ggplot(rdf_ip_top3, aes(x = as.factor(Issuer_Precinct), y = rec_count, fill = factor(FiscalYr))) + 
  geom_col(width = 0.5) + 
  facet_wrap(~FiscalYr, scale = 'free') +
  xlab("Issuer Precinct") +
  ylab("No. of Parking Tickets (In Thousands)") +
  theme(axis.text.x = element_text(angle = 90), legend.position = 'none') +
  #scale_x_discrete(limits = c('FY2015', 'FY2016', 'FY2017')) +
  ggtitle("Yearwise Parking Tickets - ") +
  labs(subtitle = 'Top 3 Issuer Precincts')

ggsave("Year Wise Issuer Precinct Frequency.jpg")


# FiscalYr Issuer_Precinct rec_count
#1 FY2015                19   552862.
#2 FY2015                18   372305.
#3 FY2015                14   360617.

#  FiscalYr Issuer_Precinct rec_count
#1 FY2016                19   523530.
#2 FY2016                18   297434.
#3 FY2016                14   293425.

#  FiscalYr Issuer_Precinct rec_count
#1 FY2017                19   383991.
#2 FY2017                14   270229.
#3 FY2017                 1   244489.

#2.4.1 Violation code frequency across Three precincts which have issue the most parking tickets
#Overall issuing frequency over 3 years will be considered to find the top 3 issuing precincts
#Issuer_Precinct 0 will not be considered for this analysis

rdf_ips_overall <- rdf_ip_smry %>% 
  filter(FiscalYr != 'OtherYr') %>%
  group_by(Issuer_Precinct) %>% 
  summarise(rec_count = sum(rec_count))
ips_order <- order(rdf_ips_overall$rec_count, decreasing = T, method = 'auto' )
head(rdf_ips_overall[ips_order,],3)

#Top Issuer Precinct codes 19, 14, 18


#2.5.1 Top Violating code across top issuer precincts
sdf_ipvc_smry <- sdf_ParkingData_new %>%   
  filter(Issue_Year == 2014 | Issue_Year == 2015 | Issue_Year == 2016 | Issue_Year == 2017) %>%
  filter(Issuer_Precinct == 19 | Issuer_Precinct == 14 | Issuer_Precinct == 18) %>%
  select(Issue_Date_2, Issuer_Precinct, Violation_Code) %>% 
  group_by(Issue_Date_2, Issuer_Precinct, Violation_Code) %>% 
  summarise(rec_count = n())
rdf_ipvc_smry <- as.data.frame(sdf_ipvc_smry)
rdf_ipvc_smry %>% head()


rdf_ipvc_smry$FiscalYr <- 'OtherYr'
rdf_ipvc_smry$FiscalYr[which(rdf_ipvc_smry$Issue_Date_2 >= '2014-10-01' & rdf_ipvc_smry$Issue_Date_2 <= '2015-09-30')] <- 'FY2015'
rdf_ipvc_smry$FiscalYr[which(rdf_ipvc_smry$Issue_Date_2 >= '2015-10-01' & rdf_ipvc_smry$Issue_Date_2 <= '2016-09-30')] <- 'FY2016'
rdf_ipvc_smry$FiscalYr[which(rdf_ipvc_smry$Issue_Date_2 >= '2016-10-01' & rdf_ipvc_smry$Issue_Date_2 <= '2017-09-30')] <- 'FY2017'
rdf_ipvc_smry %>% head()


rdf_ipvcs <- rdf_ipvc_smry %>% 
  group_by(FiscalYr, Issuer_Precinct, Violation_Code) %>% 
  summarise(rec_count = sum(rec_count))

ipvc_top3_2015_ip14 <- get_yrwise_smry('FY2015', rdf_ipvcs[which(rdf_ipvcs$Issuer_Precinct == 14),], 3)
ipvc_top3_2015_ip18 <- get_yrwise_smry('FY2015', rdf_ipvcs[which(rdf_ipvcs$Issuer_Precinct == 18),], 3)
ipvc_top3_2015_ip19 <- get_yrwise_smry('FY2015', rdf_ipvcs[which(rdf_ipvcs$Issuer_Precinct == 19),], 3)

ipvc_top3_2016_ip14 <- get_yrwise_smry('FY2016', rdf_ipvcs[which(rdf_ipvcs$Issuer_Precinct == 14),], 3)
ipvc_top3_2016_ip18 <- get_yrwise_smry('FY2016', rdf_ipvcs[which(rdf_ipvcs$Issuer_Precinct == 18),], 3)
ipvc_top3_2016_ip19 <- get_yrwise_smry('FY2016', rdf_ipvcs[which(rdf_ipvcs$Issuer_Precinct == 19),], 3)


ipvc_top3_2017_ip14 <- get_yrwise_smry('FY2017', rdf_ipvcs[which(rdf_ipvcs$Issuer_Precinct == 14),], 3)
ipvc_top3_2017_ip18 <- get_yrwise_smry('FY2017', rdf_ipvcs[which(rdf_ipvcs$Issuer_Precinct == 18),], 3)
ipvc_top3_2017_ip19 <- get_yrwise_smry('FY2017', rdf_ipvcs[which(rdf_ipvcs$Issuer_Precinct == 19),], 3)


ipvc_top3_2015 <- rbind(ipvc_top3_2015_ip14, ipvc_top3_2015_ip18)
ipvc_top3_2015 <- rbind(ipvc_top3_2015, ipvc_top3_2015_ip19)

ipvc_top3_2016 <- rbind(ipvc_top3_2016_ip14, ipvc_top3_2016_ip18)
ipvc_top3_2016 <- rbind(ipvc_top3_2016, ipvc_top3_2016_ip19)

ipvc_top3_2017 <- rbind(ipvc_top3_2017_ip14, ipvc_top3_2017_ip18)
ipvc_top3_2017 <- rbind(ipvc_top3_2017, ipvc_top3_2017_ip19)

rdf_ipvc_top3 <- rbind(ipvc_top3_2015, ipvc_top3_2016)
rdf_ipvc_top3 <- rbind(rdf_ipvc_top3, ipvc_top3_2017)
rdf_ipvc_top3$rec_count <- rdf_ipvc_top3$rec_count / 1000

rdf_ipvc_top3

ggplot(rdf_ipvc_top3, aes(x = as.factor(Violation_Code), y = rec_count, fill = factor(FiscalYr))) + 
  geom_col(width = 0.5) + 
  facet_wrap(FiscalYr~Issuer_Precinct, scale = 'free_x') +
  xlab("Violation Code") +
  ylab("No. of Parking Tickets (In Thousands)") +
  theme(axis.text.x = element_text(angle = 90), legend.position = 'none') +
  #scale_x_discrete(limits = c('FY2015', 'FY2016', 'FY2017')) +
  ggtitle("Yearwise Parking Tickets - ") +
  labs(subtitle = 'Top3 Issuer Precincts, Top 3 Violation Codes')

ggsave("Year Wise Issuer Precinct Top 3 Violation Codes Frequency.jpg")



#2.6.1 Violation time analysis
sdf_VT_smry <- sdf_ParkingData_new %>%   
  filter(Issue_Year == 2014 | Issue_Year == 2015 | Issue_Year == 2016 | Issue_Year == 2017) %>%
  select(Issue_Date_2, VT_hour, VT_AMPM, Violation_Code) %>% 
  group_by(Issue_Date_2,  VT_hour, VT_AMPM, Violation_Code) %>% 
  summarise(rec_count = n())
rdf_VT_smry <- as.data.frame(sdf_VT_smry)
rdf_VT_smry %>% head()


rdf_VT_smry$FiscalYr <- 'OtherYr'
rdf_VT_smry$FiscalYr[which(rdf_VT_smry$Issue_Date_2 >= '2014-10-01' & rdf_VT_smry$Issue_Date_2 <= '2015-09-30')] <- 'FY2015'
rdf_VT_smry$FiscalYr[which(rdf_VT_smry$Issue_Date_2 >= '2015-10-01' & rdf_VT_smry$Issue_Date_2 <= '2016-09-30')] <- 'FY2016'
rdf_VT_smry$FiscalYr[which(rdf_VT_smry$Issue_Date_2 >= '2016-10-01' & rdf_VT_smry$Issue_Date_2 <= '2017-09-30')] <- 'FY2017'
rdf_VT_smry %>% head()



#Time Zone

rdf_VTs <- rdf_VT_smry %>% 
  group_by(FiscalYr, VT_AMPM, VT_hour, Violation_Code) %>% 
  summarise(rec_count = sum(rec_count))


rdf_VT_smry$TimeZone <- '*'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'A' & rdf_VT_smry$VT_hour == 12 )] <- 'Late Night'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'A' & rdf_VT_smry$VT_hour == 1 )] <- 'Late Night'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'A' & rdf_VT_smry$VT_hour == 2 )] <- 'Late Night'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'A' & rdf_VT_smry$VT_hour == 3 )] <- 'Late Night'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'A' & rdf_VT_smry$VT_hour == 4 )] <- 'Early Morning'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'A' & rdf_VT_smry$VT_hour == 5 )] <- 'Early Morning'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'A' & rdf_VT_smry$VT_hour == 6 )] <- 'Early Morning'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'A' & rdf_VT_smry$VT_hour == 7 )] <- 'Early Morning'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'A' & rdf_VT_smry$VT_hour == 8 )] <- 'Peak Hour'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'A' & rdf_VT_smry$VT_hour == 9 )] <- 'Peak Hour'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'A' & rdf_VT_smry$VT_hour == 10 )] <- 'Peak Hour'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'A' & rdf_VT_smry$VT_hour == 11 )] <- 'Peak Hour'

rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'P' & rdf_VT_smry$VT_hour == 12 )] <- 'After Noon'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'P' & rdf_VT_smry$VT_hour == 1 )] <- 'After Noon'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'P' & rdf_VT_smry$VT_hour == 2 )] <- 'After Noon'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'P' & rdf_VT_smry$VT_hour == 3 )] <- 'After Noon'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'P' & rdf_VT_smry$VT_hour == 4 )] <- 'Evening'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'P' & rdf_VT_smry$VT_hour == 5 )] <- 'Evening'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'P' & rdf_VT_smry$VT_hour == 6 )] <- 'Evening'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'P' & rdf_VT_smry$VT_hour == 7 )] <- 'Evening'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'P' & rdf_VT_smry$VT_hour == 8 )] <- 'Night'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'P' & rdf_VT_smry$VT_hour == 9 )] <- 'Night'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'P' & rdf_VT_smry$VT_hour == 10 )] <- 'Night'
rdf_VT_smry$TimeZone[which(rdf_VT_smry$VT_AMPM == 'P' & rdf_VT_smry$VT_hour == 11 )] <- 'Night'

#Summarize as per TimeZone, Violation Code
rdf_timezone_vc_smry <- rdf_VT_smry %>%
  filter(TimeZone != '*') %>%
  group_by(FiscalYr, TimeZone, Violation_Code) %>%
  summarise(rec_count = sum(rec_count))
rdf_timezone_vc_smry %>% head

get_timezone_topvc <- function(df, Yr, tz, topcount){
  df1 <- df %>% filter(FiscalYr == Yr , TimeZone == tz)
  df1_smry_sort <- order(df1$rec_count, decreasing = T, method = 'auto' )
  return(head(df1[df1_smry_sort,],topcount))
}

#Top 3 violation code as per the time zone
vt_top3_2015_ph <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2015',  'Peak Hour', 3)
vt_top3_2015_an <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2015',  'After Noon', 3)
vt_top3_2015_ev <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2015',  'Evening', 3)
vt_top3_2015_nt <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2015',  'Night', 3)
vt_top3_2015_ln <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2015',  'Late Night', 3)
vt_top3_2015_em <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2015',  'Early Morning', 3)

vt_top3_2016_ph <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2016',  'Peak Hour', 3)
vt_top3_2016_an <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2016',  'After Noon', 3)
vt_top3_2016_ev <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2016',  'Evening', 3)
vt_top3_2016_nt <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2016',  'Night', 3)
vt_top3_2016_ln <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2016',  'Late Night', 3)
vt_top3_2016_em <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2016',  'Early Morning', 3)

vt_top3_2017_ph <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2017',  'Peak Hour', 3)
vt_top3_2017_an <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2017',  'After Noon', 3)
vt_top3_2017_ev <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2017',  'Evening', 3)
vt_top3_2017_nt <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2017',  'Night', 3)
vt_top3_2017_ln <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2017',  'Late Night', 3)
vt_top3_2017_em <- get_timezone_topvc(rdf_timezone_vc_smry, 'FY2017',  'Early Morning', 3)

vt_top3_2015 <- rbind(vt_top3_2015_ph, vt_top3_2015_an)
vt_top3_2015 <- rbind(vt_top3_2015, vt_top3_2015_ev)
vt_top3_2015 <- rbind(vt_top3_2015, vt_top3_2015_nt)
vt_top3_2015 <- rbind(vt_top3_2015, vt_top3_2015_ln)
vt_top3_2015 <- rbind(vt_top3_2015, vt_top3_2015_em)


vt_top3_2016 <- rbind(vt_top3_2016_ph, vt_top3_2016_an)
vt_top3_2016 <- rbind(vt_top3_2016, vt_top3_2016_ev)
vt_top3_2016 <- rbind(vt_top3_2016, vt_top3_2016_nt)
vt_top3_2016 <- rbind(vt_top3_2016, vt_top3_2016_ln)
vt_top3_2016 <- rbind(vt_top3_2016, vt_top3_2016_em)


vt_top3_2017 <- rbind(vt_top3_2017_ph, vt_top3_2017_an)
vt_top3_2017 <- rbind(vt_top3_2017, vt_top3_2017_ev)
vt_top3_2017 <- rbind(vt_top3_2017, vt_top3_2017_nt)
vt_top3_2017 <- rbind(vt_top3_2017, vt_top3_2017_ln)
vt_top3_2017 <- rbind(vt_top3_2017, vt_top3_2017_em)


rdf_vt_top3 <- rbind(vt_top3_2015, vt_top3_2016)
rdf_vt_top3 <- rbind(vt_top3, vt_top3_2017)

View(rdf_vt_top3)
rdf_vt_top3$rec_count <- rdf_vt_top3$rec_count/1000

ggplot(rdf_vt_top3, aes(x = as.factor(Violation_Code), y = rec_count, fill = factor(FiscalYr))) + 
  geom_col(width = 0.5) + 
  facet_wrap(FiscalYr~TimeZone, nrow=3, ncol=6, scales = 'free') +
  xlab("Violation Code") +
  ylab("No. of Parking Tickets (In Thousands)") +
  theme(axis.text.x = element_text(angle = 90), legend.position = 'none') +
  #scale_x_discrete(limits = c('FY2015', 'FY2016', 'FY2017')) +
  ggtitle("Yearwise Parking Tickets - ") +
  labs(subtitle = 'TimeZone wise Top 3 Violation Codes')

ggsave("TimeZone wise Top 3 Violation Codes Frequency.jpg")



#5.2 For the most commonly occuring time zone find the most common times of day
#Top 3 violation codes

vc <- rdf_VT_smry %>% group_by(Violation_Code) %>% summarise(rec_count = n())
vc_order <- order(vc$rec_count, decreasing = T, method = 'auto')
head(vc[vc_order,], 3)

#Top violation code 46, 40, 14

get_vc_toptz <- function(df, Yr, vc, topcount){
  df1 <- df %>% filter(FiscalYr == Yr , Violation_Code == vc)
  df1_smry_sort <- order(df1$rec_count, decreasing = T, method = 'auto' )
  return(head(df1[df1_smry_sort,],topcount))
}

vc_toptz_2015_46 <- get_vc_toptz(rdf_timezone_vc_smry, 'FY2015', 46, 3)
vc_toptz_2015_40 <- get_vc_toptz(rdf_timezone_vc_smry, 'FY2015', 40, 3)
vc_toptz_2015_14 <- get_vc_toptz(rdf_timezone_vc_smry, 'FY2015', 14, 3)

vc_toptz_2016_46 <- get_vc_toptz(rdf_timezone_vc_smry, 'FY2016', 46, 3)
vc_toptz_2016_40 <- get_vc_toptz(rdf_timezone_vc_smry, 'FY2016', 40, 3)
vc_toptz_2016_14 <- get_vc_toptz(rdf_timezone_vc_smry, 'FY2016', 14, 3)

vc_toptz_2017_46 <- get_vc_toptz(rdf_timezone_vc_smry, 'FY2017', 46, 3)
vc_toptz_2017_40 <- get_vc_toptz(rdf_timezone_vc_smry, 'FY2017', 40, 3)
vc_toptz_2017_14 <- get_vc_toptz(rdf_timezone_vc_smry, 'FY2017', 14, 3)

vc_toptz_2015 <- rbind(vc_toptz_2015_14, vc_toptz_2017_40)
vc_toptz_2015 <- rbind(vc_toptz_2015, vc_toptz_2017_46)

vc_toptz_2016 <- rbind(vc_toptz_2016_14, vc_toptz_2017_40)
vc_toptz_2016 <- rbind(vc_toptz_2016, vc_toptz_2017_46)

vc_toptz_2017 <- rbind(vc_toptz_2017_14, vc_toptz_2017_40)
vc_toptz_2017 <- rbind(vc_toptz_2017, vc_toptz_2017_46)

rdf_vc_toptz <- rbind(vc_toptz_2015, vc_toptz_2016)
rdf_vc_toptz <- rbind(rdf_vc_toptz, vc_toptz_2017)
View(rdf_vc_toptz)
rdf_vc_toptz$rec_count <- rdf_vc_toptz$rec_count/1000

ggplot(rdf_vc_toptz, aes(x = as.factor(Violation_Code), y = rec_count, fill = factor(FiscalYr))) + 
  geom_col(width = 0.5) + 
  facet_grid(FiscalYr~TimeZone, scales = 'free') +
  xlab("Violation Code") +
  ylab("No. of Parking Tickets (In Thousands)") +
  theme(axis.text.x = element_text(angle = 90), legend.position = 'none') +
  #scale_x_discrete(limits = c('FY2015', 'FY2016', 'FY2017')) +
  ggtitle("Yearwise Parking Tickets - ") +
  labs(subtitle = 'Violation Codes as per Time Zone')

ggsave("Violation Codes as per Time Zone.jpg")


#2.6 Analysis of Seasonality in the data
sdf_Season_smry <- sdf_ParkingData_new %>%   
  filter(Issue_Year == 2014 | Issue_Year == 2015 | Issue_Year == 2016 | Issue_Year == 2017) %>%
  select(Issue_Date_2, Issue_Month, Violation_Code) %>% 
  group_by(Issue_Date_2,  Issue_Month, Violation_Code) %>% 
  summarise(rec_count = n())
rdf_Season_smry <- as.data.frame(sdf_Season_smry)
rdf_Season_smry %>% head()


rdf_Season_smry$FiscalYr <- 'OtherYr'
rdf_Season_smry$FiscalYr[which(rdf_Season_smry$Issue_Date_2 >= '2014-10-01' & rdf_Season_smry$Issue_Date_2 <= '2015-09-30')] <- 'FY2015'
rdf_Season_smry$FiscalYr[which(rdf_Season_smry$Issue_Date_2 >= '2015-10-01' & rdf_Season_smry$Issue_Date_2 <= '2016-09-30')] <- 'FY2016'
rdf_Season_smry$FiscalYr[which(rdf_Season_smry$Issue_Date_2 >= '2016-10-01' & rdf_Season_smry$Issue_Date_2 <= '2017-09-30')] <- 'FY2017'
rdf_Season_smry %>% head()

#Seasons
rdf_Season_smry$Season = '*'
rdf_Season_smry$Season[which(rdf_Season_smry$Issue_Month == 1)] = 'Winter'
rdf_Season_smry$Season[which(rdf_Season_smry$Issue_Month == 2)] = 'Winter'
rdf_Season_smry$Season[which(rdf_Season_smry$Issue_Month == 3)] = 'Spring'
rdf_Season_smry$Season[which(rdf_Season_smry$Issue_Month == 4)] = 'Spring'
rdf_Season_smry$Season[which(rdf_Season_smry$Issue_Month == 5)] = 'Spring'
rdf_Season_smry$Season[which(rdf_Season_smry$Issue_Month == 6)] = 'Summer'
rdf_Season_smry$Season[which(rdf_Season_smry$Issue_Month == 7)] = 'Summer'
rdf_Season_smry$Season[which(rdf_Season_smry$Issue_Month == 8)] = 'Summer'
rdf_Season_smry$Season[which(rdf_Season_smry$Issue_Month == 9)] = 'Autumn'
rdf_Season_smry$Season[which(rdf_Season_smry$Issue_Month == 10)] = 'Autumn'
rdf_Season_smry$Season[which(rdf_Season_smry$Issue_Month == 11)] = 'Autumn'
rdf_Season_smry$Season[which(rdf_Season_smry$Issue_Month == 12)] = 'Winter'

#Summarize as per Seasons
rdf_season_vc_smry <- rdf_Season_smry%>%
  filter(Season != '*') %>%
  group_by(FiscalYr, Season, Violation_Code) %>%
  summarise(rec_count = sum(rec_count))
rdf_season_vc_smry %>% head

get_season_topvc <- function(df, Yr, Sn, topcount){
  df1 <- df %>% filter(FiscalYr == Yr , Season == Sn)
  df1_smry_sort <- order(df1$rec_count, decreasing = T, method = 'auto' )
  return(head(df1[df1_smry_sort,],topcount))
}

#Top 3 violation code as per the Season
sn_topvc_2015_smr <- get_season_topvc(rdf_season_vc_smry, 'FY2015',  'Summer', 3)
sn_topvc_2015_atm <- get_season_topvc(rdf_season_vc_smry, 'FY2015',  'Autumn', 3)
sn_topvc_2015_wtr <- get_season_topvc(rdf_season_vc_smry, 'FY2015',  'Winter', 3)
sn_topvc_2015_spr <- get_season_topvc(rdf_season_vc_smry, 'FY2015',  'Spring', 3)

sn_topvc_2016_smr <- get_season_topvc(rdf_season_vc_smry, 'FY2016',  'Summer', 3)
sn_topvc_2016_atm <- get_season_topvc(rdf_season_vc_smry, 'FY2016',  'Autumn', 3)
sn_topvc_2016_wtr <- get_season_topvc(rdf_season_vc_smry, 'FY2016',  'Winter', 3)
sn_topvc_2016_spr <- get_season_topvc(rdf_season_vc_smry, 'FY2016',  'Spring', 3)

sn_topvc_2017_smr <- get_season_topvc(rdf_season_vc_smry, 'FY2017',  'Summer', 3)
sn_topvc_2017_atm <- get_season_topvc(rdf_season_vc_smry, 'FY2017',  'Autumn', 3)
sn_topvc_2017_wtr <- get_season_topvc(rdf_season_vc_smry, 'FY2017',  'Winter', 3)
sn_topvc_2017_spr <- get_season_topvc(rdf_season_vc_smry, 'FY2017',  'Spring', 3)

sn_topvc_2015 <- rbind(sn_topvc_2015_smr, sn_topvc_2015_atm)
sn_topvc_2015 <- rbind(sn_topvc_2015, sn_topvc_2015_wtr)
sn_topvc_2015 <- rbind(sn_topvc_2015, sn_topvc_2015_spr)

sn_topvc_2016 <- rbind(sn_topvc_2016_smr, sn_topvc_2016_atm)
sn_topvc_2016 <- rbind(sn_topvc_2016, sn_topvc_2016_wtr)
sn_topvc_2016 <- rbind(sn_topvc_2016, sn_topvc_2016_spr)

sn_topvc_2017 <- rbind(sn_topvc_2017_smr, sn_topvc_2017_atm)
sn_topvc_2017 <- rbind(sn_topvc_2017, sn_topvc_2017_wtr)
sn_topvc_2017 <- rbind(sn_topvc_2017, sn_topvc_2017_spr)


rdf_sn_topvc <- rbind(sn_topvc_2015, sn_topvc_2016)
rdf_sn_topvc <- rbind(rdf_sn_topvc, sn_topvc_2017)
View(rdf_sn_topvc)
rdf_sn_topvc$rec_count <- rdf_sn_topvc$rec_count/1000000

ggplot(rdf_sn_topvc, aes(x = as.factor(Violation_Code), y = rec_count, fill = factor(FiscalYr))) + 
  geom_col(width = 0.5) + 
  facet_grid(FiscalYr~Season, scales = 'free_x') +
  xlab("Violation Code") +
  ylab("No. of Parking Tickets (In Millions)") +
  theme(axis.text.x = element_text(angle = 90), legend.position = 'none') +
  #scale_x_discrete(limits = c('FY2015', 'FY2016', 'FY2017')) +
  ggtitle("Yearwise Parking Tickets - ") +
  labs(subtitle = 'Violation Codes as per Seasons')

ggsave("Violation Codes as per Seasons.jpg")




#2.7 Analysis of taxes paid
rdf_vc_codes <- as.data.frame(sdf_codes)
rdf_vcs %>% head
rdf_vc_codes %>% head

help(merge)
rdf_vcs <- merge(rdf_vcs, rdf_vc_codes, by = 'Violation_Code')
rdf_vcs %>% head(10)

rdf_vcs$TaxesPaid = (as.double(rdf_vcs$rec_count) * as.double(rdf_vcs$Fine_Amount))

rdf_vcs %>% 
  filter(FiscalYr != 'OtherYr') %>%
  group_by(FiscalYr) %>%
  summarise(Total_Taxes_Paid = sum(TaxesPaid))

vc_taxes <- rdf_vcs %>% 
  filter(FiscalYr != 'OtherYr') %>%
  group_by(Violation_Code) %>%
  summarise(rec_count = sum(rec_count), Total_Taxes_Paid = sum(TaxesPaid))

vc_order <- order(vc_taxes$rec_count, decreasing = T, method = c("auto"))

rdf_vc_taxes <- head(vc_taxes[vc_order,], 3)
rdf_vc_taxes

#Violation_Code rec_count Total_Taxes_Paid
#1             21  4130579.       227181845.
#2             36  3236274.       161813700.
#3             38  3162499.       158124950.
