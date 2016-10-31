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
StratNamesURL<-paste("https://dev.macrostrat.org/api/defs/strat_names?all&format=csv")
GotURL<-getURL(StratNamesURL)
UnitsFrame<-read.csv(text=GotURL,header=TRUE)

# Create unit dictionary
CandidateUnits<-unique(as.character(UnitsFrame[,"strat_name_long"]))

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
# NUMBER OF CANDIDATE UNITS FOUND IN TUPLES MATCHED IN SUBSETDEEPDIVE
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
# NUMBER OF ROWS (SENTENCES) IN SUBSETDEEPDIVE WITH UNIT NAME HITS OF CANDIDATE UNITS AFTER REMOVING SENTENCES WHICH CONTAIN MORE THAN ONE CANDIDATE UNIT NAME
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
# NUMBER OF SHORT SENTENCES IN SUBSETDEEPDIVE WITH SINGLE CANDIDATE UNIT HITS AND NO MACROUNITDICTIONARY NAMES
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
# NUMBER OF UNIQUE ROWS FROM SUBSETDEEPDIVE
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
# NUMBER OF UNIQUE ROWS FROM SUBSETDEEPDIVE
StepSevenRows<-length(unique(ReservoirData[,"MatchLocation"]))
# NUMBER OF UNIT MATCHES 
StepSevenUnits<-length(unique(ReservoirData[,"UnitName"]))

# Create a final data frame for the output
# Extract the document id data for each match
DocID<-sapply(ReservoirData[,"MatchLocation"], function(x) DeepDiveData[x,"docid"])
# Extract the sentence id data for each match
SentID<-sapply(ReservoirData[,"MatchLocation"], function(x) DeepDiveData[x,"sentid"])
    
# Remove unnecessary data from the final output data frame
OutputData<-ReservoirData[,c("UnitName","Sentence")]
# bind the unit name match, sentence, document id, and sentence id data into a data frame
OutputData<-cbind(OutputData,DocID,SentID)
    
# Return stats table 
StepDescription<-c(StepOneDescription, StepTwoDescription, StepThreeDescription, StepFourDescription, StepFiveDescription, StepSixDescription, StepSevenDescription)
NumberDocuments<-c(StepOneDocs, StepTwoDocs, StepThreeDocs, StepFourDocs, StepFiveDocs, StepSixDocs, StepSevenDocs)
NumberRows<-c(StepOneRows, StepTwoRows, StepThreeRows, StepFourRows, StepFiveRows, StepSixRows, StepSevenRows)
NumberUnits<-c(StepOneUnits, StepTwoUnits, StepThreeUnits, StepFourUnits, StepFiveUnits, StepSixUnits, StepSevenUnits)
    
Stats<-cbind(StepDescription,NumberDocuments,NumberRows,NumberUnits)
    
# Stop the cluster
stopCluster(Cluster)
    
print(paste("Writing Outputs",Sys.time()))
    
CurrentDirectory<-getwd()
setwd(paste(CurrentDirectory,"/output",sep=""))
    
saveRDS(UnitHitData, "UnitHitData.rds")
write.csv(UnitHitData, "UnitHitData.csv")
write.csv(Stats,"Stats.csv",row.names=FALSE)
saveRDS(OutputData,"Reservoir_OutputData.rds")
write.csv(OutputData,"Reservoir_OutputData.csv")

    
print(paste("Complete",Sys.time()))
