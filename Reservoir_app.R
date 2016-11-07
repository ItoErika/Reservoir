Start<-print(Sys.time())

# Install libraries if necessary and load them into the environment
if (require("RCurl",warn.conflicts=FALSE)==FALSE) {
    install.packages("RCurl",repos="http://cran.cnr.berkeley.edu/");
    library("RCurl");
    }
    
if (require("doParallel",warn.conflicts=FALSE)==FALSE) {
    install.packages("doParallel",repos="http://cran.cnr.berkeley.edu/");
    library("doParallel");
    }

if (require("RPostgreSQL",warn.conflicts=FALSE)==FALSE) {
    install.packages("RPostgreSQL",repos="http://cran.cnr.berkeley.edu/");
    library("RPostgreSQL");
    }
    
# Start a cluster for multicore, 4 by default or higher if passed as command line argument
CommandArgument<-commandArgs(TRUE)
if (is.na(CommandArgument)) {
    Cluster<-makeCluster(4)
    } else {
    Cluster<-makeCluster(as.numeric(CommandArgument[1]))
    }

# Download the config file
Credentials<-as.matrix(read.table("Credentials.yml",row.names=1))

print(paste("Begin loading postgres tables.",Sys.time()))

# Connet to PostgreSQL
Driver <- dbDriver("PostgreSQL") # Establish database driver
Connection <- dbConnect(Driver, dbname = Credentials["database:",], host = Credentials["host:",], port = Credentials["port:",], user = Credentials["user:",])
# STEP ONE: Load DeepDiveData 
# Make SQL query
DeepDiveData<-dbGetQuery(Connection,"SELECT docid, sentid, words FROM nlp_sentences_352")

# RECORD INITIAL STATS
# INITIAL NUMBER OF DOCUMENTS AND ROWS IN DEEPDIVEDATA: 
StepOneDescription<-"Initial Data"
# INITIAL NUMBER OF DOCUMENTS AND ROWS IN DEEPDIVEDATA:
StepOneDocs<-length((unique(DeepDiveData[,"docid"])))
StepOneRows<-nrow(DeepDiveData)
StepOneUnits<-"NA"

# STEP TWO: Download geologic unit names from Macrostrat Database
#download strat_name_long data from Macrostrat
UnitsURL<-paste("https://dev.macrostrat.org/api/units?project_id=1&response=long&format=csv")
GotURL<-getURL(UnitsURL)
UnitsFrame<-read.csv(text=GotURL,header=TRUE)

# Create unit dictionary
CandidateUnits<-unique(as.character(UnitsFrame[,"strat_name_long"]))
# Remove blank CandidateUnits element
CandidateUnits<-CandidateUnits[!(CandidateUnits%in%"")]

# RECORD INITIAL STATS
StepTwoDescription<-"Download Macrostrat unit dictionary"
# INITIAL NUMBER OF DOCUMENTS AND ROWS IN DEEPDIVEDATA:
StepTwoDocs<-StepOneDocs
StepTwoRows<-StepOneRows
StepTwoUnits<-length(CandidateUnits)

# STEP THREE: Search for candidate units in DeepDiveData.
# Remove symbols 
DeepDiveData[,"words"]<-gsub("\\{|\\}","",DeepDiveData[,"words"])
# Remove commas from DeepDiveData
CleanedWords<-gsub(","," ",DeepDiveData[,"words"])

# Search for the long unit names in CleanedWords
print(paste("Begin search for candidate units.",Sys.time()))
UnitHits<-parSapply(Cluster,CandidateUnits,function(x,y) grep(x,y,ignore.case=FALSE, perl = TRUE),CleanedWords)
print(paste("Finish search for candidate units.",Sys.time()))

# Create a matrix of unit hits locations with corresponding unit names
UnitHitsLength<-sapply(UnitHits,length)
UnitName<-rep(names(UnitHits),times=UnitHitsLength)
UnitHitData<-cbind(UnitName,unlist(UnitHits))
# convert matrix to data frame
UnitHitData<-as.data.frame(UnitHitData)
# Name column denoting row locations within Cleaned Words
colnames(UnitHitData)[2]<-"MatchLocation"
# convert row match location (denotes location in CleanedWords) column to numerical data
UnitHitData[,"MatchLocation"]<-as.numeric(as.character(UnitHitData[,"MatchLocation"]))
    
# RECORD STATS
StepThreeDescription<-"Search for candidate units in DeepDiveData"
# NUMBER OR DOCUMENTS IN DEEPDIVEDATA WITH UNIT NAME HITS OF CANDIDATE UNITS FOUND IN TUPLES
StepThreeDocs<-length(unique(DeepDiveData[UnitHitData[,"MatchLocation"],"docid"]))
# NUMBER OF ROWS IN DEEPDIVEDATA WITH CANDIDATE UNIT HITS
StepThreeRows<-length(unique(UnitHitData[,"MatchLocation"]))
# NUMBER OF CANDIDATE UNITS FOUND IN DEEPDIVEDATA
StepThreeUnits<-length(unique(names(UnitHits[which(sapply(UnitHits,length)>0)])))   

# STEP FOUR: Eliminate rows/sentences from DeepDiveData which contain more than one candidate unit name
# Make a table showing the number of unit names which occur in each DeepDiveData row that we know has at least one unit match
RowHitsTable<-table(UnitHitData[,"MatchLocation"])
# Locate and extract rows which contain only one long unit
# Remember that the names of RowHitsTable correspond to rows within CleanedWords
SingleHits<-as.numeric(names(RowHitsTable)[which((RowHitsTable)==1)])    
# Subset LongUnitData to get dataframe of Cleaned Words rows and associated single hit long unit names
SingleHitData<-subset(UnitHitData,UnitHitData[,"MatchLocation"]%in%SingleHits==TRUE)
# Create a column of sentences from CleanedWords and bind it to SingleHitData
Sentence<-CleanedWords[SingleHitData[,"MatchLocation"]]
SingleHitData<-cbind(SingleHitData,Sentence)   
    
# RECORD STATS
StepFourDescription<-"Eliminate sentences with more than one candidate unit name" 
# NUMBER OF DOCUMENTS OF INTEREST AFTER NARROWING DOWN TO ROWS WITH ONLY ONE CANDIDATE UNIT
StepFourDocs<-length(unique(DeepDiveData[SingleHitData[,"MatchLocation"],"docid"]))
# NUMBER OF ROWS (SENTENCES) IN DEEPDIVE WITH UNIT NAME HITS OF CANDIDATE UNITS AFTER REMOVING SENTENCES WHICH CONTAIN MORE THAN ONE CANDIDATE UNIT NAME
StepFourRows<-length(unique(SingleHitData[,"MatchLocation"]))
# NUMBER OF UNIT MATCHES AFTER NARROWING DOWN TO ROWS WITH ONLY ONE CANDIDATE UNIT
StepFourUnits<-length(unique(SingleHitData[,"UnitName"]))  
    
# STEP 5: Eliminate rows/sentences that are more than 350 characters in length.
# Find the character length for each character string in SingleHitData sentences
Chars<-sapply(SingleHitData[,"Sentence"], function (x) nchar(as.character(x)))
# bind the number of characters for each sentence to UnitData
UnitData<-cbind(SingleHitData,Chars)
# Locate the rows which have UnitData sentences with less than or equal to 350 characters
ShortSents<-which(UnitData[,"Chars"]<=350)
UnitDataCut<-SingleHitData[ShortSents,] 

# RECORD STATS
StepFiveDescription<-"Eliminate sentences >350 characters in length"
# NUMBER OF DOCUMENTS OF INTEREST AFTER CUTTING OUT LONG ROWS
StepFiveDocs<-length(unique(DeepDiveData[UnitDataCut[,"MatchLocation"],"docid"]))
# NUMBER OF SHORT SENTENCES IN DEEPDIVEDATA WITH SINGLE CANDIDATE UNIT HITS AND NO MACROUNITDICTIONARY NAMES
StepFiveRows<-length(unique(UnitDataCut[,"MatchLocation"]))
# NUMBER OF UNIT MATCHES AFTER NARROWING DOWN TO ONLY SHORT ROWS 
StepFiveUnits<-length(unique(UnitDataCut[,"UnitName"]))

# STEP 6: Search for words the word "aquifer".
# Search for the word "aquifer" in UnitDataCut sentences 
# Record start time
print(paste("Begin search for unit and aquifer matches.",Sys.time()))
AquiferHits<-grep("aquifer",UnitDataCut[,"Sentence"], ignore.case=TRUE, perl=TRUE)
# Record end time
print(paste("Finish search for unit and aquifer matches.",Sys.time()))

# Subset SingleHitsCut to only rows with aquifer sentences
AquiferData<-unique(UnitDataCut[AquiferHits,])  
    
# RECORD STATS
StepSixDescription<-"Search for the word 'aquifer'"
# NUMBER OF DOCUMENTS OF INTEREST 
StepSixDocs<-length(unique(DeepDiveData[AquiferData[,"MatchLocation"],"docid"]))
# NUMBER OF UNIQUE ROWS FROM TDEEPDIVEData
StepSixRows<-length(unique(AquiferData[,"MatchLocation"]))
# NUMBER OF UNIT MATCHES 
StepSixUnits<-length(unique(AquiferData[,"UnitName"]))

# STEP 7: Search for and remove words that create noise in the data ("underlying","overlying","overlain", "overlie", "overlies", "underlain", "underlie", and "underlies")
# Record start time
print(paste("Begin search for unwanted matches.",Sys.time()))
# NOTE: removing "underlie" and "overlie" should also get rid of "underlies" and "overlies"
Overlain<-grep("overlain",AquiferData[,"Sentence"], ignore.case=TRUE, perl=TRUE)
Overlie<-grep("overlie",AquiferData[,"Sentence"], ignore.case=TRUE, perl=TRUE)
Overlying<-grep("overlying",AquiferData[,"Sentence"], ignore.case=TRUE, perl=TRUE)
Underlain<-grep("underlain",AquiferData[,"Sentence"], ignore.case=TRUE, perl=TRUE)
Underlie<-grep("underlie",AquiferData[,"Sentence"], ignore.case=TRUE, perl=TRUE)
Underlying<-grep("underlying",AquiferData[,"Sentence"], ignore.case=TRUE, perl=TRUE)

# Combine all of the noisy rows (sentences) into one vector 
NoisySentences<-c(Overlain,Overlie,Underlain,Underlie,Underlying,Overlying)
    
# Remove noisy sentences from AquiferData
ReservoirData<-AquiferData[-NoisySentences,]
# Record end time
print(paste("Finish removing unwanted matches.",Sys.time()))

# RECORD STATS
StepSevenDescription<-"Remove sentences with noisy words"
# NUMBER OF DOCUMENTS OF INTEREST 
StepSevenDocs<-length(unique(DeepDiveData[ReservoirData[,"MatchLocation"],"docid"]))
# NUMBER OF UNIQUE ROWS FROM DEEPDIVEDATA
StepSevenRows<-length(unique(ReservoirData[,"MatchLocation"]))
# NUMBER OF UNIT MATCHES 
StepSevenUnits<-length(unique(ReservoirData[,"UnitName"]))
    
# Create a data frame for the output
# Extract the document id data for each match
DocID<-sapply(ReservoirData[,"MatchLocation"], function(x) DeepDiveData[x,"docid"])
# Extract the sentence id data for each match
SentID<-sapply(ReservoirData[,"MatchLocation"], function(x) DeepDiveData[x,"sentid"])
    
# Remove unnecessary data from the final output data frame
OutputData<-ReservoirData[,c("UnitName","Sentence")]
# bind the unit name match, sentence, document id, and sentence id data into a data frame
OutputData<-cbind(OutputData,DocID,SentID)
    
# STEP 8: Clean and subset the output. Try to varify that the unit matches are valid by searching for their locations.
print(paste("Begin location check.",Sys.time()))
# Remove all rows from UnitsFrame with blank "strat_name_long" columns
UnitsFrame<-UnitsFrame[which(nchar(as.character(UnitsFrame[,"strat_name_long"]))>0),]
# Subset UnitsFrame so it only includes matched units from ReservoirData
CandidatesFrame<-UnitsFrame[which(as.character(UnitsFrame[,"strat_name_long"])%in%OutputData[,"UnitName"]),]
# Load col_id, location tuple data
LocationTuples<-read.csv("LocationTuples.csv")
# Join the territory names to CandidatesFrame
CandidatesFrame<-merge(CandidatesFrame,LocationTuples, by="col_id",all.x="TRUE")
CandidatesFrame<-unique(CandidatesFrame)   
# Extracts columns of interest
CandidatesFrame<-CandidatesFrame[,c("strat_name_long","col_id","location")] 

# rename the output data column of unit names so OutputData can be merged with location data for the units
colnames(OutputData)[1]<-"strat_name_long"
# Create a table of unit data that has the unit name, docid of the match, and the location(s) the unit is known to be in.
# NOTE: this merge will create a row for each location/col_id tuple associated with each match.
UnitOutputData<-merge(OutputData,CandidatesFrame, by="strat_name_long", all.x=TRUE)    

# Search for the locations from UnitOutputData[,"location"] column in DeepDiveData documents are referenced by docid in UnitOutputData

locationSearch<-function(DeepDiveData,Document=UnitOutputData[,"DocID"], location=unique(UnitOutputData[,"location"])){
    # subset DeepDiveData to only documents referenced in OutputData
    SubsetDeepDive<-subset(DeepDiveData, DeepDiveData[,"docid"]%in%Document)
    # clean sentences so grep can run
    CleanedWords<-gsub(","," ",SubsetDeepDive[,"words"])
    # search for locations in SubsetDeepDive
    LocationHits<-sapply(location, function (x,y) grep (x,y, ignore.case=TRUE,perl=TRUE), CleanedWords)
    # make a column of location names for each associated hit
    LocationHitsLength<-sapply(LocationHits,length)
    names(LocationHits)<-unique(UnitOutputData[,"location"])
    Location<-rep(names(LocationHits),times=LocationHitsLength)
    # make a column for each document the location name is found in
    LocationDocs<-SubsetDeepDive[unlist(LocationHits),"docid"]
    # create an output matrix which contains each location and the document in which it appears
    return(cbind(LocationDocs,Location))
    }

# run the locationSearch function
LocationHits<-locationSearch(DeepDiveData,Document=UnitOutputData[,"DocID"], location=unique(UnitOutputData[,"location"]))
LocationHits<-unique(LocationHits)    

# Create a version of UnitOutputData with just docid and location names
UnitDocLocation<-as.matrix(UnitOutputData[,c("DocID","location")])  
# Make a column of docid and location data combined for LocationHits and UnitDocLocation
Doc.Location1<-paste(LocationHits[,"LocationDocs"],LocationHits[,"Location"],sep=".")
Doc.Location2<-paste(UnitDocLocation[,"DocID"],UnitDocLocation[,"location"],sep=".")
# Bind these columns to each respective matrix
LocationHits<-cbind(LocationHits, Doc.Location1)
UnitDocLocation<-cbind(UnitDocLocation, Doc.Location2)
                         
# Find the rows from UnitOutputData that are also in LocationHits to varify that the correct location appears in the document with the unit assocoiated with the location.
# NOTE: this removes all rows associated with unit matches which do not have the correct location mentioned in the document
CheckedOutputData<-UnitOutputData[which(UnitDocLocation[,"Doc.Location2"]%in%LocationHits[,"Doc.Location1"]),]
# remove duplicate rows of strat name, sentence, docid, and sentid data that were created from the location data merge
FinalOutputData<-unique(CheckedOutputData[,c("strat_name_long","Sentence","DocID","SentID")])
                         
print(paste("Finish location check.",Sys.time()))                        
                         
# RECORD STATS
StepEightDescription<-"Validate unit locations"
# NUMBER OF DOCUMENTS OF INTEREST 
StepEightDocs<-length(unique(FinalOutputData[,"DocID"]))
# NUMBER OF UNIQUE ROWS FROM DEEPDIVEDATA
StepEightRows<-length(unique(FinalOutputData[,"Sentence"]))
# NUMBER OF UNIT MATCHES 
StepEightUnits<-length(unique(FinalOutputData[,"strat_name_long"]))   
                         
# Return stats table 
StepDescription<-c(StepOneDescription, StepTwoDescription, StepThreeDescription, StepFourDescription, StepFiveDescription, StepSixDescription, StepSevenDescription, StepEightDescription)
NumberDocuments<-c(StepOneDocs, StepTwoDocs, StepThreeDocs, StepFourDocs, StepFiveDocs, StepSixDocs, StepSevenDocs, StepEightDocs)
NumberRows<-c(StepOneRows, StepTwoRows, StepThreeRows, StepFourRows, StepFiveRows, StepSixRows, StepSevenRows, StepEightRows)
NumberUnits<-c(StepOneUnits, StepTwoUnits, StepThreeUnits, StepFourUnits, StepFiveUnits, StepSixUnits, StepSevenUnits, StepEightUnits)
    
Stats<-cbind(StepDescription,NumberDocuments,NumberRows,NumberUnits)
    
# Stop the cluster
stopCluster(Cluster)
    
print(paste("Writing Outputs",Sys.time()))
    
CurrentDirectory<-getwd()
setwd(paste(CurrentDirectory,"/output",sep=""))
    
saveRDS(UnitHitData, "UnitHitData.rds")
write.csv(UnitHitData, "UnitHitData.csv")
write.csv(Stats,"Stats.csv",row.names=FALSE)
saveRDS(FinalOutputData,"Reservoir_OutputData.rds")
write.csv(FinalOutputData,"Reservoir_OutputData.csv")

    
print(paste("Complete",Sys.time()))
