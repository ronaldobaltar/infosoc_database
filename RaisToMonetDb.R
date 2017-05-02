library(MonetDb)
library(MonetDBLite)
library(DBI)
library(data.table)
library(car) #recode


#Style convetion for this code scripit: Variables created by me start with capital letter
#and are separeted by undersline score not dots.

#Path to monetdblite database for infosoc named as imndb

Db_Dir <-  file.path("C:", "Users", "Ronaldo", "OneDrive", "rprojs", "infosoc_d", "infosoc_d", "imndb")


Con <- dbConnect(MonetDBLite::MonetDBLite(), Db_Dir)

#Files to insert fo monetDb 
#
#The directory is organized by years
Files_Dir <-  file.path("C:", "Users", "Ronaldo", "OneDrive", "dados", "Rais_Txt")

File_List <- dir(Files_Dir) 

#For test purpose only
File_List_Test <- "AC2015.txt"

#your_data_frame <- do.call(rbind,lapply(file_names,read.csv))
#
#
#to do: indetinfy the estabelecimento file 
#
for (Current_File in File_List_Test) {
  
 #Note: reading a csv file from a url address
 #url <- "http://www.address.com/file.csv"
 #FileName <- fread(url)
 #
 #
 #important to rename manualy the variable names, because the original file contains
 #many non US characters (portuguese characters). In order to handle the variables
 #is important to keep the names (known as 'idetifiers' in monetdbLite sql) as lowercase.
  Rais_Col_Names_2 = c(
    "bairros_sp",          #1
    "bairros_fortaleza",   #2
    "bairros_rj",          #3
    "causa_afastamento_1", #4
    "causa_afastamento_2", #5
    "causa_afastamento_3", #6  
    "motivo_desligamento", #7
    "cbo_ocupacoes_2002",  #8
    "cnae_2_0_classe",     #9      
    "cnae_95_classe",      #10
    "distritos_sp",        #11
    "vinculo_ativo_31_12", #12  
    "faixa_etaria",        #13
    "faixa_hora_contrat",  #14
    "faixa_remun_dezem_sm",#15  
    "faixa_remun_media_sm",#16
    "faixa_tempo_emprego", #17
    "gr_instrucao",        #18
    "qtd_hora_contr",      #19 num
    "idade",               #20 num
    "ind_cei_vinculado",   #21      
    "ind_simples",         #22 num
    "mes_admissao",        #23
    "mes_desligamento",    #24 num
    "mun_trab",            #25
    "municipio",           #26
    "nacionalidade",       #27       
    "natureza_juridica",   #28
    "ind_portador_defic",  #29
    "qtd_dias_afastamento",#30 num
    "raca_cor",            #31
    "regioes_adm_df",      #32
    "remun_dezembro_nom",  #33 num
    "remun_dezembro_sm",   #34 num
    "remun_media_nom",     #34 num
    "remun_media_sm",      #35 num
    "cnae_2_0_subclasse",  #36 
    "sexo_trabalhador",    #37 
    "tamanho_estab",       #38
    "tempo_emprego",       #39 num
    "tipo_admissao",       #40
    "tipo_estab",          #41     
    "tipo_estab_1",        #42
    "tipo_deficiencia",    #43
    "tipo_vinculo",        #44
    "ibge_subsetor",       #45
    "vi_rem_janeiro_cc",   #46 num
    "vi_rem_fevereiro_cc", #47 num     
    "vi_rem_marco_cc",     #48 num
    "vi_rem_abril_cc",     #49 num
    "vi_rem_maio_cc",      #49 num
    "vi_rem_junho_cc",     #50 num
    "vi_rem_julho_cc",     #51 num
    "vi_rem_agosto_cc",    #52 num 
    "vi_rem_setembro_cc",  #53 num
    "vi_rem_outubro_cc",   #54 num
    "vi_rem_novembro_cc"   #55 num
  )
  
  
#for test purpose only
  Current_File <- File_List_Test 
  
#Convert csv to data.table
 DataTableFile <- fread(paste0(Files_Dir,"/",Current_File), 
                        header = TRUE, sep = ";", dec = ",", check.names = TRUE,
                        data.table = FALSE,
                        encoding = "UTF-8", col.name = Rais_Col_Names_2,
                        colClasses = list(character = c(1:18,21,23,25:29,31:32,36:38,40:45))
                        )  
 
 #recode
 DataTableFile <- within(DataTableFile, {
   raca_cor <- Recode(raca_cor, 
                              '"1" = "indigena"; "2" = "branca"; "4" = "preta"; "6" = "amarela"; "8" = "parda"; "9" = "nao ident"; "-1" = "ignorado"', as.factor.result = FALSE)
 })
 
 DataTableFile <- within(DataTableFile, {
   sexo_trabalhador <- Recode(sexo_trabalhador, 
                      '"1" = "masculino"; "2" = "feminino"; "-1" = "ignorado"', as.factor.result = FALSE)
 })
 
 
  #write current data.table to monetdblite table
  dbWriteTable(Con, "rais2015", DataTableFile, header = TRUE, overwrite = TRUE )
 
  # add a new columns to indetify the year and brazilian state code
  dbSendQuery(Con, "ALTER TABLE rais2015 ADD COLUMN ano INTEGER" )
  dbSendQuery(Con, "ALTER TABLE rais2015 ADD COLUMN uf VARCHAR(2)" )
  
 # populate that new column with year (ano) and brazlian state code (uf)
  dbSendQuery(Con, "UPDATE rais2015 SET ano = 2015" )
  dbSendQuery(Con, "UPDATE rais2015 SET uf = 'AC'")
  
 } #end for loop


#test if the line where inserted
dbGetQuery(Con, "SELECT raca_cor, COUNT(*) FROM rais2015 GROUP BY raca_cor;" )
dbGetQuery(Con, "SELECT nacionalidade, raca_cor, sexo_trabalhador FROM rais2015 LIMIT 10;" )
dbGetQuery(Con, "SELECT uf, ano, rem_janeiro_cc,rem_novembro_cc FROM rais2015 LIMIT 100;" )

#dbGetQuery(Con, "DROP TABLE rais2015")
dbListTables(Con)
dbListFields(Con, "rais2015" )
