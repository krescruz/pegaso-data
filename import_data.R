library("plyr")

BASE_DIR <- getwd()

# Definition of final columns
COLUMNAMES <- c(
	'Folio',
	'TRANS',
	'UUID',
	'COCIA',
	'TNOPR',
	'ANIOPR',
	'NUMPR',
	'NEMP',
	'RFC',
	'MTCON',
	'MTING',
	'CCONC',
	'IMPTE',
	'IMGRA',
	'IMEXC'
)

# List file names
FILES <- c(
	paste(BASE_DIR, 'file_name_report_20170331.csv', sep='/'),
)

YEAR <- 2018

# Deposit result in the directory /DATA RESULT
for (file_origin in FILES) {
	  # Define path name
	  path_final = paste(BASE_DIR, '/DATA RESULT/', 'DATA_',
	  					 substr(file_origin, nchar(file_origin)-7,
	  					 nchar(file_origin)), sep='')
	  path_otros = paste(BASE_DIR, '/DATA RESULT/', 'DATA_OTROSPAG_',
	  					 substr(file_origin, nchar(file_origin)-7,
	  					 nchar(file_origin)), sep='')
	  #Init process
	  resulData(file_origin, path_final, path_otros)
}


resulData <- function(path_origin, path_perded, path_otrospag){
	sat <- read.csv(path_origin, encoding="UTF-8")

	message("Deleting rows empty")
	# Clean rows footer
	sat <- sat[!is.na(sat$Folio), ]
	sat <- unique(sat)

	message("Total rows: ", nrow(sat))

	# LIMPIAR columnas generales
	sat$Serie <- as.character(sat$X.U.FEFF.Serie) # sat$Serie
	sat$UUID <- as.character(sat$FolioFiscal)
	sat$TRANS <- as.character(sat$FolioERP)
	sat$NEMP <- as.character(sat$NumEmpleado)
	sat$RFC <- as.character(sat$RfcReceptor)
	sat$MTING <- 'X'
	sat$IMPTE <- 0


	# Separate value for columns
	dataSerie <- getDFSerie(sat$Serie)
	sat <- mergeDataSerie(sat, dataSerie)

	#Separate data for years
	sat.year <- getDataYear(sat, YEAR)


	# Geting data
	sat.percepciones <- getDFPercepciones(sat.year)
	sat.deducciones <- getDFDeducciones(sat.year)
	sat.mov <- join(sat.percepciones, sat.deducciones, type = "full")
	sat.mov <- sat.mov[order(sat.mov$TNOPR, sat.mov$NUMPR, sat.mov$NEMP), ]
	# Save data
	saveCsv(sat.mov, path_perded)

	# Getting and save if exist other payments
	if (nrow(sat.year[!is.na(sat.year$OtrClaveOtroPago), ])>0) {
	  sat.otrosconceptos <- getDFOtrosPag(sat.year)
	  saveCsv(sat.otrosconceptos, path_otrospag)
	}

}


saveCsv <- function(dataval, path){
	write.csv(
		dataval,
        file=path,
        na="", quote=TRUE,  row.names=FALSE)
}

getDataYear <- function(dataframe, year) {
	return (dataframe[dataframe$"ANIOPR" == year, ])
}

mergeDataSerie <- function(data1, data2){
	return (cbind(data1, data2))
}

getDFSerie <- function(columnSerie) {
	dataSerie <- ldply(strsplit(columnSerie, "-"))
	colnames(dataSerie) <- c("COCIA", "TNOPR", "ANIOPR", "NUMPR")
	return (dataSerie)
}

getDFPercepciones <- function(dataframe) {
	df = (dataframe[!is.na(dataframe$PerClavePercepcion), ])
	df$MTCON = 'P'
	df$CCONC <- as.character(df$PerClavePercepcion)
	df$MTING = NULL
	df$MTING[df$PerImporteGravado > 0 & df$PerImporteExento == 0] <- 'G'
	df$MTING[df$PerImporteExento > 0 & df$PerImporteGravado == 0] <- 'E'
	df$MTING[df$PerImporteGravado > 0 & df$PerImporteExento > 0] <- 'T'

	df$IMGRA = df$PerImporteGravado
	df$IMEXC = df$PerImporteExento
	df$IMPTE = df$IMGRA  +df$IMEXC
	df = df[COLUMNAMES]
  	return (df)
}

getDFDeducciones <- function(dataframe) {
  	df = (dataframe[!is.na(dataframe$DedClaveDeduccion), ])
	df$MTCON = 'D'
	df$CCONC <- as.character(df$DedClaveDeduccion)
	df$IMGRA = 0
	df$IMEXC = 0
	df$MTING = 'X'
	df$IMPTE = df$DedImporte
	df = df[COLUMNAMES]
  	return (df)
}

getDFOtrosPag <- function(dataframe) {
  	df = (dataframe[!is.na(dataframe$OtrClaveOtroPago), ])
	df$MTCON = ''
	df$CCONC <- as.character(df$OtrClaveOtroPago)
	df$IMGRA = 0
	df$IMEXC = 0
	df$MTING = ''
	df$IMPTE = df$OtrImporteOtroPago
	df = df[COLUMNAMES]
  	return (df)
}
