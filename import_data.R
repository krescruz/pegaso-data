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

# Lista file names
FILES <- c(
	paste(BASE_DIR, 'file_name_report_20170331.csv', sep='/'),
)

# Deposit result in the directory /DATA RESULT
for (file_origin in FILES) {
  # Nombre de archivo Final
  path_final = paste(BASE_DIR, '/DATA RESULT/', 'DATA_',
  					 substr(file_origin, nchar(file_origin)-7,
  					 nchar(file_origin)), sep='')
  path_otros = paste(BASE_DIR, '/DATA RESULT/', 'DATA_OTROSPAG_',
  					 substr(file_origin, nchar(file_origin)-7,
  					 nchar(file_origin)), sep='')
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

	sat.2017 <- getDataYear(sat, 2017)
	sat.2016 <- getDataYear(sat, 2016)
	sat.2018 <- getDataYear(sat, 2018)
	

	# Geting data
	sat.percepciones <- getDFPercepciones(sat.2017)
	sat.deducciones <- getDFDeducciones(sat.2017)
	sat.mov <- join(sat.percepciones, sat.deducciones, type = "full")
	sat.mov <- sat.mov[order(sat.mov$TNOPR, sat.mov$NUMPR, sat.mov$NEMP), ]
	# Save data
	saveCsv(sat.mov, path_perded)

	# Getting and save if exist other payments
	if (nrow(sat.2017[!is.na(sat.2017$OtrClaveOtroPago), ])>0) {
	  sat.otrosconceptos <- getDFOtrosPag(sat.2017)
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
