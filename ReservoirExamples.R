# Custom functions are camelCase. Arrays, parameters, and arguments are PascalCase
# Dependency functions are not embedded in master functions
# []-notation is used wherever possible, and $-notation is avoided.


######################################## Load Required Libraries ###########################################

# Load Required Libraries
library("RPostgreSQL")
library("doParallel")
library("pbapply")
#Designate PBDB as source
source("https://raw.githubusercontent.com/aazaff/paleobiologyDatabase.R/master/communityMatrix.R")

##################################### Establish postgresql connection #######################################

# Remember to start postgres (pgadmin) if it is not already running
Driver <- dbDriver("PostgreSQL") # Establish database driver
Connection <- dbConnect(Driver, dbname = "erikaito", host = "localhost", port = 5432, user = "erikaito")

# Load Data Table Into R 
DeepDiveData<-dbGetQuery(Connection,"SELECT * FROM pyrite_example_data")

###################################### Establish DeepDive Dictionaries ######################################

#Select words of interest
ReservoirDictionary<-c("aquifer","reservoir","aquitard","aquiclude","water","oil","gas","coal","aquifuge")
# Make vector of upper case words and add it to the original vector
ReservoirDictionary<-c(ReservoirDictionary,gsub("(^[[:alpha:]])", "\\U\\1", ReservoirDictionary, perl=TRUE))

# Additional possibilities 
# c("permeable","porous","ground water","permeability","porosity","unsaturated zone","vadose zone","water table")

# Download dictionary of unit names from Macrostrat Database
UnitsURL<-paste("https://dev.macrostrat.org/api/units?project_id=1&format=csv")
GotURL<-getURL(UnitsURL)
UnitsFrame<-read.csv(text=GotURL,header=TRUE)
# Extract actual unit names
UnitsDictionary<-unique(UnitsFrame[,"unit_name"])

######################################## Establish DeepDive Functions #######################################

# Function for identifying the parent of matches
findParents<-function(DocRow,FirstDictionary=ReservoirDictionary) {
    CleanedWords<-gsub("\\{|\\}","",DocRow["words"])
    CleanedParents<-gsub("\\{|\\}","",DocRow["dep_parents"])
    SplitParents<-unlist(strsplit(CleanedParents,","))
  	SplitWords<-unlist(strsplit(CleanedWords,","))
  	FoundWords<-SplitWords%in%FirstDictionary
  	
  	# Create coumns for final matrix
  	MatchedWords<-SplitWords[which(FoundWords)]
  	FoundParents<-SplitWords[as.numeric(SplitParents[which(FoundWords)])]
  	DocumentID<-rep(DocRow["docid"],length(MatchedWords))
  	SentenceID<-rep(DocRow["sentid"],length(MatchedWords))
  	
  	# Account for words with missing parents 
  	if (length(FoundParents)<1) {
  	    FoundParents<-rep(NA,length(MatchedWords))
  	    }
  	    
  	# Return the function output
  	return(cbind(MatchedWords,FoundParents,DocumentID,SentenceID))
  	}
  	
DDResults<-pbapply(DeepDiveData[1:1000,],1,findParents,PyriteDictionary)
ParentMatches<-sapply(DDResults,length)
# Rbind the matrix together
DDResultsMatrix<-do.call(rbind,DDResults)

#F################## Find sentences with words from both dictionaries in DeepDiveData ########################

#Use a for loop to extract desired sentences from DeepDiveData
#Remember to store sentences as a data frame because there is both numeric and text data
DDSentences<-data.frame(matrix(NA,nrow=nrow(DDResultsMatrix),ncol=ncol(DeepDiveData)))
for (Row in 1:nrow(DDResultsMatrix)) {
    DDSentences[Row,]<-subset(DeepDiveData,DeepDiveData[,"docid"]==DDResultsMatrix[Row,"DocumentID"] & DeepDiveData[,"sentid"]==as.numeric(DDResultsMatrix[Row,"SentenceID"]))
    }

################################################# DeepDive Analyses ##########################################

# Find all sentences with matches in initial dictionary (i.e., Reservoir Dictionary)
ReservoirSentences<-pbapply(DeepDiveData,1,findParents)

# Extract only matching sentences
ReservoirMatches<-sapply(ReservoirSentences,nrow)
ReservoirSentences<-ReservoirSentences[which(ReservoirMatches!=0)]





############################################ Step by step examples ############################################

# Get the IDs of unique documents
Documents<-unique(DDResultsMatrix[,"DocumentID"])

# Get document data of DDResultsMatrix from DeepDiveData
Documents<-subset(DeepDiveData,DeepDiveData[,"docid"]%in%unique(DDResultsMatrix[,"DocumentID"]))

# Subset DeepDiveData to get a sentence of interest 
subset(DeepDiveData,DeepDiveData[,"docid"]==DDResultsMatrix[1,"DocumentID"] & DeepDiveData[,"sentid"]==as.numeric(DDResultsMatrix[1,"SentenceID"]))





    





