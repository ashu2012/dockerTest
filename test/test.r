library(dplyr)
library(dplyr)
library(purrr)
library(stringr)
library(lubridate)
library(jsonlite)
library(caTools)
library(rapportools)
library(readr)
#give score file path
file_scoring <- file.path("C:\\Users\\h217119\\Documents\\cps\\testing\\model_service.r")

source(file_scoring)

#json_input <- fromJSON("C:\\Users\\h217119\\Documents\\cps\\testing\\ReferenceinputJsonOn2017-11-20.json") %>% toJSON()



json_input <- read_file("C:\\Users\\h217119\\Documents\\cps\\testing\\ReferenceinputJsonOn2017-11-20.json")

#encodes json file
json_input_enc <- json_input %>% base64_enc()

lstt <- service(json_input_enc)

lstt<-lstt%>%base64_dec() %>% rawToChar()
write_file(lstt %>% prettify(), "output.json") 



