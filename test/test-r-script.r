
#give score file path
file_scoring <- file.path("score path")

source(file_scoring)

json_input <- fromJSON("input json path") %>% toJSON()

#encodes json file
json_input_enc <- json_input %>% base64_enc()

lstt <- score(json_input_enc)%>%base64_dec() %>% rawToChar() 
write_file(lstt %>% prettify(), "outputpath")