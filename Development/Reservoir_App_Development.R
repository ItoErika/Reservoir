# Load Libraries
library("RPostgreSQL")
library("doParallel")
library("pbapply")
library("RCurl")
library("data.table")

#Connet to PostgreSQL
#Driver <- dbDriver("PostgreSQL") # Establish database driver
#Connection <- dbConnect(Driver, dbname = "labuser", host = "localhost", port = 5432, user = "labuser")

#DeepDiveData<-dbGetQuery(Connection,"SELECT * FROM aquifersentences_nlp352_master")

# Load DeepDiveData 
DeepDiveData<- fread("~/Documents/DeepDive/Reservoir/R/DeepDiveData.csv")
DeepDiveData<-as.data.frame(DeepDiveData)

# Extract columns of interest from DeepDiveData
GoodCols<-c("docid","sentid","wordidx","words","poses","dep_parents")
DeepDiveData<-DeepDiveData[,(names(DeepDiveData)%in%GoodCols)]

# Remove symbols 
DeepDiveData[,"words"]<-gsub("\\{|\\}","",DeepDiveData[,"words"])
DeepDiveData[,"poses"]<-gsub("\\{|\\}","",DeepDiveData[,"poses"])
# Make a substitute for commas so they are counted correctly as elements for future functions
DeepDiveData[,"words"]<-gsub("\",\"","COMMASUB",DeepDiveData[,"words"])
DeepDiveData[,"poses"]<-gsub("\",\"","COMMASUB",DeepDiveData[,"poses"])

# Download dictionary of unit names from Macrostrat Database
# UnitsURL<-paste("https://dev.macrostrat.org/api/defs/strat_names?all&format=csv")
# GotURL<-getURL(UnitsURL)
# UnitsFrame<-read.csv(text=GotURL,header=TRUE)

#Extract a dictionary of long strat names from UnitsFrame
# UnitDictionary<-UnitsFrame[,"strat_name_long"]
# Add spaces to the front and back
# UnitDictionary<-pbsapply(UnitDictionary,function(x) paste(" ",x," ",collapse=""))

UnitsURL<-paste("https://dev.macrostrat.org/api/units?project_id=1&format=csv&response=long")
GotURL<-getURL(UnitsURL)
UnitsFrame<-read.csv(text=GotURL,header=TRUE)

StratNamesURL<-paste("https://dev.macrostrat.org/api/defs/strat_names?all&format=csv")
GotURL<-getURL(StratNamesURL)
StratNamesFrame<-read.csv(text=GotURL,header=TRUE)

# Get rid of columns from UnitsFrame that are also in StratNamesFrame so the merge function can run smoothly EXCEPT "strat_name_id"
UnitsFrameNoOverlap<-UnitsFrame[,!(names(UnitsFrame) %in% names(StratNamesFrame))]
UnitsFrame<-UnitsFrame[,c(colnames(UnitsFrameNoOverlap),"strat_name_id")]

Units<-merge(x = StratNamesFrame, y = UnitsFrame, by = "strat_name_id", all.x = TRUE)

# unit_id, col_id, lat, lng, unit_name, strat_name_long

Units<-Units[,c("strat_name_id","strat_name_long","strat_name","unit_id","unit_name","col_id","t_age","b_age","max_thick","min_thick","lith")]

Units<-na.omit(Units)

# Add a column of mean thickness
Units$mean_thick<-apply(Units,1,function(row) mean(row["max_thick"]:row["min_thick"]))
    
# Make two dictionaries: one of long strat names and one of short strat names

# make a matrix of long and short units only. 
LongShortUnits<-Units[,c("strat_name_long","strat_name")]
# duplicate rows
LongShortUnits<-unique(LongShortUnits)

# Create dictionaries
LongUnitDictionary<-as.character(LongShortUnits[,"strat_name_long"])
ShortUnitDictionary<-as.character(LongShortUnits[,"strat_name"])

# commas from DeepDiveData
CleanedWords<-gsub(","," ",DeepDiveData[,"words"])

Cluster<-makeCluster(4)

Start<-print(Sys.time())
LongUnitHits<-parSapply(Cluster,LongUnitDictionary,function(x,y) grep(x,y,ignore.case=FALSE, perl = TRUE),CleanedWords)
End<-print(Sys.time())

# Create a matrix of unit hits locations with corresponding unit names
LongUnitHitsLength<-pbsapply(LongUnitHits,length)
LongUnitNames<-rep(names(LongUnitHits),times=LongUnitHitsLength)
LongUnitLookUp<-cbind(LongUnitNames,unlist(LongUnitHits))
# convert matrix to data frame
LongUnitLookUp<-as.data.frame(LongUnitLookUp)
# convert row match location (denotes location in CleanedWords) column to numerical data
colnames(LongUnitLookUp)[2]<-"MatchLocation"
LongUnitLookUp[,"MatchLocation"]<-as.numeric(as.character(LongUnitLookUp[,"MatchLocation"]))
    
AcceptableHits<-names(table(LongUnitLookUp))[which(table(LongUnitLookUp[,"MatchLocation"])==1)]
LongHitTable<-subset(LongUnitLookUp,LongUnitLookUp[,"MatchLocation"]%in%AcceptableHits==TRUE)
    
# Remove MatchLocation rows which appear more than once (remove sentences in which more than one unit occurs)

    
    
# names(UnitHits)<-LongUnitDictionary
    
Start<-print(Sys.time())
ShortUnitHits<-parSapply(Cluster,ShortUnitDictionary,function(x,y) grep(x,y,ignore.case=FALSE, perl = TRUE),CleanedWords)
End<-print(Sys.time())
    
stopCluster(Cluster)

# names(UnitHits)<-ShortUnitDictionary
    
    
# Locate documents in which matches occurred for each respective strat_name_long

# Unlist the row location data for each element(unit) in LongUnitHits
MatchRowList<-lapply(LongUnitHits,function(x) unlist(x))
# Extract docid data associated with each row in MatchRowList
MatchDocList<-lapply(MatchRowList,function(x) DeepDiveData[x,"docid"])  

    
# Find sentences within documents that have long unit names (MatchDocList) that also contain the word "aquifer"
# Note that this does not sub out spaces for commas, because we are doing a single word search    
findPairs<-function(DeepDiveData,MatchDocList,ShortUnitHits,Word="aquifer") {
    FinalVector<-vector("logical",length=length(MatchDocList))
    names(FinalVector)<-names(MatchDocList)
    progbar<-txtProgressBar(min=0,max=length(FinalVector),style=3)
    for (i in 1:length(FinalVector)) {
        Temp<-DeepDiveData[ShortUnitHits[[i]],]
        DocSubset<-subset(Temp,Temp[,"docid"]%in%MatchDocList[[i]]==TRUE)
        FinalVector[i]<-length(grep(Word,DocSubset[,"words"],ignore.case=TRUE))
        setTxtProgressBar(progbar,i)
        }
    close(progbar)
    return(FinalVector)
    }
    
Matches<-findPairs(DeepDiveData[,c("docid","words")],MatchDocList,ShortUnitHits,"aquifer")    
    
# Search documents in MatchDocList for abbreviated unit names of the full names for which they have hits

Units[which(Units[,"strat_name_long"]==as.character(names(MatchDocList[1]))),]


# Remove empty List elements
NumHits<-pbsapply(MatchRowList,length) 
UnitSentences<-MatchRowList[which(NumHits>0)]
    
Start<-print(Sys.time())
AquiferMatches<-pbsapply(UnitSentences,function(x,y) grep("aquifer",y[x],ignore.case=TRUE, perl = TRUE),CleanedWords)
End<-print(Sys.time())
    
AquiferHits<-pbsapply(AquiferMatches,length)
# Remove units with no hits 
# AquiferHits<-AquiferHits[which(AquiferHits[names(AquiferHits)]!=0)]
AquiferSentences<-AquiferMatches[which(AquiferHits>0)]
    
# SEE TEST1 in Reservoir_Tests page for accuracy test
    
# Extract macrostrat data for each unit with aquifer hits
    
# Make a vector of aquifer names
Aquifers<-names(AquiferSentences)
    
# Extract Aquifer data from Units dataframe by name
AquiferUnits<-Units[which(Units[,"strat_name_long"]%in%Aquifers),]
    

  





    





    
    
    
    
    
    
    
    
    
    
    
    
    
    

DeepDiveData.dt<-data.table(DeepDiveData)

# Create a function to that will search for unit names in DeepDiveData documents
findGrep<-function(Words,Dictionary) {
    CleanedWords<-gsub(","," ",Words)
    Match<-grep(Dictionary,CleanedWords,ignore.case=TRUE)
    return(Match)    
    }

findUnits<-function(DeepDiveData,Dictionary) {
    FinalList<-vector("list",length=length(Dictionary))
    names(FinalList)<-Dictionary
    progbar<-txtProgressBar(min=0,max=length(Dictionary),style=3)
    for (Unit in 1:length(Dictionary)) {
        FinalList[[Unit]]<-DeepDiveData[,sapply(.SD,findGrep,Dictionary[Unit]),by="docid",.SDcols="words"]
        setTxtProgressBar(progbar,Unit)
        }
    close(progbar)
    return(FinalList)
    }
 
UnitHits<-findUnits(DeepDiveData.dt,UnitDictionary)

# Establish a cluster for doParallel
# Make Core Cluster
#Cluster<-makeCluster(4)
# Pass the functions to the cluster
#clusterExport(cl=Cluster,varlist="findGrep")
    
# Search for unit names in DeepDiveData documents
findUnits<-function(Document,Dictionary) {
    CleanedWords<-gsub(","," ",Document["words"]) 
    #UnitHits<-parSapply(Cluster,Dictionary,findGrep,CleanedWords)
    UnitHits<-sapply(Dictionary,findGrep,CleanedWords)
    return(UnitHits)
    }

# Apply function to DeepDiveData documents
# Start<-print(Sys.time())
# UnitHits<-by(DeepDiveData,DeepDiveData[,"docid"],findUnits,UnitDictionary)
# End<-print(Start-Sys.time())

DeepDiveData.dt<-data.table(DeepDiveData[1:6000,])
UnitHits<-DeepDiveData.dt[,sapply(.SD,findUnits,UnitDictionary),by="docid",.SDcols=c("docid","words")]

# forloop

# Start<-print(Sys.time())
# hitUnits<-function(DeepDiveData,UnitDictionary) {
       #Documents<-unique(DeepDiveData[,"docid"])
       #FinalList<-vector("list",length=length(Documents))
       #progbar<-txtProgressBar(min=0,max=length(Documents),style=3)
       #for (Document in Documents) {
           #Subset<-subset(DeepDiveData,DeepDiveData[,"docid"]==Document)
           #FinalList[[Document]]<-findUnits(Subset,UnitDictionary)
           #setTxtProgressBar(progbar,Document)
           #}
       #close(progbar)
       #return(FinalList)
       #}

# Run batch loop on all DeepDiveDocuments
# UnitHits<-hitUnits(DeepDiveData[1:500,],UnitDictionary)
# End<-print(Start-Sys.time())

# Stop the cluster
# stopCluster(Cluster)
    




















##################################### Organize into Data Frame #########################################

# Create matrix of DDResults
DDResultsMatrix<-do.call(rbind,DDResults)
rownames(DDResultsMatrix)<-1:dim(DDResultsMatrix)[1]
DDResultsFrame<-as.data.frame(DDResultsMatrix)

# Ensure all columns are in correct format
DDResultsFrame[,"MatchedWord"]<-as.character(DDResultsFrame[,"MatchedWord"])
DDResultsFrame[,"WordPosition"]<-as.numeric(as.character(DDResultsFrame[,"WordPosition"]))
DDResultsFrame[,"Pose"]<-as.character(DDResultsFrame[,"Pose"])
DDResultsFrame[,"DocumentID"]<-as.character(DDResultsFrame[,"DocumentID"])
DDResultsFrame[,"SentenceID"]<-as.numeric(as.character(DDResultsFrame[,"SentenceID"]))

#################################### Isolate NNPs ############################################# 

# Subset so you only get NNP matches
DDResultsFrame<-subset(DDResultsFrame,DDResultsFrame[,3]=="NNP")

####################### Find NNps adjacent to DDResultsFrame Matches ##########################

# Pair document ID and sentence ID data for DeepDiveData
doc.sent<-paste(DeepDiveData[,"docid"],DeepDiveData[,"sentid"],sep=".")
# Make paired document and sentence data the row names for DeepDiveData
rownames(DeepDiveData)<-doc.sent
# Pair document ID and sentence ID data for 
docID.sentID<-paste(DDResultsFrame[,"DocumentID"],DDResultsFrame[,"SentenceID"],sep=".")
# Make a new column for paired document and sentence data in DDResultsFrame
DDResultsFrame[,"doc.sent"]=docID.sentID

# Create a subset of DeepDiveData that only contains data for document sentence pairs from DDResultsFrame
DDMatches<-DeepDiveData[unique(DDResultsFrame[,"doc.sent"]),]

# Make function to find NNP words in DDMatches
findNNPs<-function(Sentence) {
    SplitPoses<-unlist(strsplit(Sentence["poses"],","))
    SplitSentences<-unlist(strsplit(Sentence["words"],","))
    NNPs<-which(SplitPoses=="NNP")
    NNPWords<-SplitSentences[NNPs]
    return(cbind(NNPs,NNPWords))
    }
    
# Apply findNNPs function to DDMatches
NNPResults<-pbapply(DDMatches,1,findNNPs)

# Create matrix of NNPResults
NNPResultsMatrix<-do.call(rbind,NNPResults)
rownames(NNPResultsMatrix)<-1:dim(NNPResultsMatrix)[1]

# Add sentence ID data to NNPResultsMatrix
# Find the number of NNP matches for each sentence
MatchCount<-sapply(NNPResults,nrow)
# Create a column for NNPResultsMatrix for sentence IDs
NNPResultsMatrix<-cbind(NNPResultsMatrix,rep(names(NNPResults),times=MatchCount))
# Name the column
colnames(NNPResultsMatrix)[3]<-"SentID"

# Convert matrix into data frame to hold different types of data
NNPResultsFrame<-as.data.frame(NNPResultsMatrix)

# Make sure data in columns are in the correct format
NNPResultsFrame[,"NNPs"]<-as.numeric(as.character(NNPResultsFrame[,"NNPs"]))
NNPResultsFrame[,"NNPWords"]<-as.character(NNPResultsFrame[,"NNPWords"])
NNPResultsFrame[,"SentID"]<-as.character(NNPResultsFrame[,"SentID"])

##################################### Find Consecutive NNPs ######################################

findConsecutive<-function(NNPPositions) {
    Breaks<-c(0,which(diff(NNPPositions)!=1),length(NNPPositions))
    ConsecutiveList<-lapply(seq(length(Breaks)-1),function(x) NNPPositions[(Breaks[x]+1):Breaks[x+1]])
    return(ConsecutiveList)
    }

# Find consecutive NNPs for each SentID
Consecutive<-tapply(NNPResultsFrame[,"NNPs"], NNPResultsFrame[,"SentID"],findConsecutive)
# Collapse the consecutive NNP clusters into single character strings
ConsecutiveNNPs<-pbsapply(Consecutive,function(y) sapply(y,function(x) paste(x,collapse=",")))

######################### Find Words Associated with Conescutive NNPs ###########################

# Create a matrix with a row for each NNP cluster
# Make a column for sentence IDs
ClusterCount<-pbsapply(ConsecutiveNNPs,length)
SentID<-rep(names(ConsecutiveNNPs),times=ClusterCount)
# Make a column for cluster elements 
ClusterPosition<-unlist(ConsecutiveNNPs)
names(ClusterPosition)<-SentID
# Make a column for the words associated with each NNP
# Get numeric elements for each NNP
NNPElements<-lapply(ClusterPosition,function(x) as.numeric(unlist(strsplit(x,","))))
names(NNPElements)<-SentID


NNPWords<-vector("character",length=length(NNPElements))
progbar<-txtProgressBar(min=0,max=length(NNPElements),style=3)
for(Document in 1:length(NNPElements)){
    ExtractElements<-NNPElements[[Document]]
    DocumentName<-names(NNPElements)[Document]
    SplitWords<-unlist(strsplit(DDMatches[DocumentName,"words"],","))
    NNPWords[Document]<-paste(SplitWords[ExtractElements],collapse=" ")
    setTxtProgressBar(progbar,Document)
    }
close(progbar)

# Bind columns into data frame 
NNPClusterMatrix<-cbind(NNPWords,ClusterPosition,SentID)
rownames(NNPClusterMatrix)<-NULL
NNPClusterFrame<-as.data.frame(NNPClusterMatrix)

# Make sure all of the columns are in the correct data format
NNPClusterFrame[,"NNPWords"]<-as.character(NNPClusterFrame[,"NNPWords"])
NNPClusterFrame[,"ClusterPosition"]<-as.character(NNPClusterFrame[,"ClusterPosition"])
NNPClusterFrame[,"SentID"]<-as.character(NNPClusterFrame[,"SentID"])


# Find NNPClusterFrame rows in which unit names appear

# Combine all member, formation, group, and supergroup names into a single vector
AllUnitsDictionary<-c(unlist(Members),unlist(Formations),unlist(Groups),unlist(Supergroups))
# Find matches of AllUnitsDictionary in the NNPClusterFrame "NNPWords" column
MatchRows<-which(NNPClusterFrame[,"NNPWords"]%in%AllUnitsDictionary)

# Subset the NNPClusterFrame so that only CompleteMatchRows are shown
MatchFrame1<-data.frame(matrix(NA,ncol = 3, nrow = length(MatchRows)),stringsAsFactors=False)
progbar<-txtProgressBar(min=0,max=length(MatchRows),style=3)
for (Row in 1:length(MatchRows)){
    MatchFrame1[Row,]<-NNPClusterFrame[as.numeric(MatchRows[Row]),]
    setTxtProgressBar(progbar,Row)
    }
    close(progbar)
# Create column names for MatchFrame1 to match NNPClusterMatrix
colnames(MatchFrame1)<-colnames(NNPClusterFrame)
    
    
    
    # Find poses 
findPoses<-function(Document,Dictionary=SplitUnits) {
    SplitPoses<-unlist(strsplit(DocRow["poses"],","))
    SplitWords<-unlist(strsplit(DocRow[,"words"],","))
  	
    
    for(Unit in length(SplitUnits)){
           FoundWords<-SplitWords%in%SplitUnits
        
    FoundWords<-SplitWords%in%Dictionary
  	
  	# Create columns for final matrix
  	MatchedWord<-SplitWords[which(FoundWords)]
  	WordPosition<-which(FoundWords)
  	Pose<-SplitPoses[which(FoundWords)]
    DocumentID<-rep(DocRow["docid"],length(MatchedWord))
    SentenceID<-rep(DocRow["sentid"],length(MatchedWord))

    # Return the function output
    return(cbind(MatchedWord,WordPosition,Pose,DocumentID,SentenceID))
    }
  	
# Apply function to DeepDiveData documents
DDResults<-pbapply(DeepDiveData[50000:100000,],1,findPoses,UnitDictionary)
    
by(DeepDiveData,DeepDiveData[,"docid"],)
    
      	# Create columns for final matrix
    MatchedUnit<-
    DocumentID<-rep(DocRow["docid"],length(MatchedWord))
    SentenceID<-rep(DocRow["sentid"],length(MatchedWord))

