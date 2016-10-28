####################### IN POSTICO: Initial Creation of Data Table (ONLY RUN ONCE) #####################

In Postico
CREATE TABLE reservoir_data
(docid text, sentid integer, wordid integer[], words text[], poses text[], ners text[], lemmas text[], dep_paths text[], dep_parents integer []);
COPY reservoir_data FROM '/Users/erikaito/Documents/Erika_DeepDive/sentences_nlp352' WITH delimiter as '	';

################################ IN R: Load Required Libraries ##########################################

# Load Libraries
library("RPostgreSQL")
library("doParallel")
library(pbapply)
library(RCurl)

################################### Establish Postgresql Connection #####################################

# Connect to postgresql (if on erikaito)
Driver <- dbDriver("PostgreSQL") # Establish database driver
Connection <- dbConnect(Driver, dbname = "erikaito", host = "localhost", port = 5432, user = "erikaito")
DeepDiveData<-dbGetQuery(Connection,"SELECT * FROM reservoir_data")

#OR (if on labuser) 
Driver <- dbDriver("PostgreSQL") # Establish database driver
Connection <- dbConnect(Driver, dbname = "labuser", host = "localhost", port = 5432, user = "labuser")
DeepDiveData<-dbGetQuery(Connection,"SELECT * FROM reservoir_data")

###################### Clean DeepDiveData (remove symbols and add substitutes) #########################

# Remove symbols 
DeepDiveData[,"words"]<-gsub("\\{|\\}","",DeepDiveData[,"words"])
DeepDiveData[,"poses"]<-gsub("\\{|\\}","",DeepDiveData[,"poses"])
# Make a substitute for commas so they are counted correctly as elements for future functions
DeepDiveData[,"words"]<-gsub("\",\"","COMMASUB",DeepDiveData[,"words"])
DeepDiveData[,"poses"]<-gsub("\",\"","COMMASUB",DeepDiveData[,"poses"])

###################################### Create Dictionaries ##############################################

#Select words of interest
ReservoirDictionary<-c("aquifer","reservoir","aquitard","aquiclude","water","oil","gas","coal","aquifuge")
# Make vector of upper case words and add it to the original vector
ReservoirDictionary<-c(ReservoirDictionary,gsub("(^[[:alpha:]])", "\\U\\1", ReservoirDictionary, perl=TRUE))

# Download dictionary of unit names from Macrostrat Database
UnitsURL<-paste("https://dev.macrostrat.org/api/units?project_id=1&format=csv")
GotURL<-getURL(UnitsURL)
UnitsFrame<-read.csv(text=GotURL,header=TRUE)

# Create a vector of all unique unit words
UnitsVector<-c(as.character(UnitsFrame[,"Mbr"]),as.character(UnitsFrame[,"Fm"]),as.character(UnitsFrame[,"Gp"]),as.character(UnitsFrame[,"SGp"]))
UnitsVector<-unique(UnitsVector)
Units<-subset(UnitsVector,UnitsVector!="")

######################## Split Unit Names in Units Vector into Individual Words ############################

#Create a dictionary list of matrices of split unit words
NumUnits<-1:length(Units)
UnitDictionary<-vector("list",length=length(Units))
for(UnitElement in NumUnits){
    UnitDictionary[[UnitElement]]<-unlist(strsplit(noquote(gsub(" ","SPLIT",Units[UnitElement])),"SPLIT"))
    }

###################### Isolate First Words in Each Word String of UnitDictionary #########################

# Pull out the first word of every matrix in the UnitDictionary list
FirstWords<-unique(sapply(UnitDictionary,function(x) x[1]))
# Minimize noise
FirstWords<-subset(FirstWords,FirstWords!="The")

############# Find Matches of FirstWords in Deep Dive Documents and the Pose for Each Match ##############

# Find matches of FirstWords in all of the DeepDiveData documents
# Get data for the word matches (word match, poses, documentID, and sentenceID)

findPoses<-function(DocRow,FirstDictionary=FirstWords) {
    SplitPoses<-unlist(strsplit(DocRow["poses"],","))
    SplitWords<-unlist(strsplit(DocRow["words"],","))
  	FoundWords<-SplitWords%in%FirstDictionary
  	
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
DDResults<-pbapply(DeepDiveData,1,findPoses,FirstWords)

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
ConsecutiveNNPs<-sapply(Consecutive,function(y) sapply(y,function(x) paste(x,collapse=",")))

######################### Find Words Associated with Conescutive NNPs ###########################

# Create a matrix with a row for each NNP cluster
# Make a column for sentence IDs
ClusterCount<-sapply(ConsecutiveNNPs,length)
SentID<-rep(names(ConsecutiveNNPs),times=ClusterCount)
# Make a column for cluster elements 
ClusterPosition<-unlist(ConsecutiveNNPs)
names(ClusterPosition)<-SentID
# Make a column for the words associated with each NNP
# Get numeric elements for each NNP
NNPElements<-lapply(ClusterPosition,function(x) as.numeric(unlist(strsplit(x,","))))
names(NNPElements)<-SentID

NNPWords<-vector("character",length=length(NNPElements))
for(Document in 1:length(NNPElements)){
    ExtractElements<-NNPElements[[Document]]
    DocumentName<-names(NNPElements)[Document]
    SplitWords<-unlist(strsplit(DDMatches[DocumentName,"words"],","))
    NNPWords[Document]<-paste(SplitWords[ExtractElements],collapse=" ")
    }

# Bind columns into data frame 
NNPClusterMatrix<-cbind(NNPWords,ClusterPosition,SentID)
rownames(NNPClusterMatrix)<-NULL
NNPClusterFrame<-as.data.frame(NNPClusterMatrix)

# Make sure all of the columns are in the correct data format
NNPClusterFrame[,"NNPWords"]<-as.character(NNPClusterFrame[,"NNPWords"])
NNPClusterFrame[,"ClusterPosition"]<-as.character(NNPClusterFrame[,"ClusterPosition"])
NNPClusterFrame[,"SentID"]<-as.character(NNPClusterFrame[,"SentID"])

####################################### Find Complete Unit Matches ##############################

# Find NNPClusterFrame rows in which full unit names appear
CompleteMatchRows<-which(NNPClusterFrame[,"NNPWords"]%in%UnitsVector)

# Subset the NNPClusterFrame so that only CompleteMatchRows are shown
MatchFrame1<-data.frame(matrix(NA,ncol = 3, nrow = length(CompleteMatchRows)),stringsAsFactors=False)
for (Row in 1:length(CompleteMatchRows)){
    MatchFrame1[Row,]<-NNPClusterFrame[as.numeric(CompleteMatchRows[Row]),]
    }
    
# Create column names for MatchFrame1 to match NNPClusterMatrix
colnames(MatchFrame1)<-colnames(NNPClusterFrame)

####################################### Find partial unit matches #################################

# Subset NNPClusterFrame so it does not include complete unit matches
# This function is the opposite of %in%
'%!in%' <- function(x,y)!('%in%'(x,y))
IncompleteMatchRows<-which(NNPClusterFrame[,"NNPWords"]%!in%UnitsVector)
SubsetFrame1<-data.frame(matrix(NA,ncol = 3, nrow = length(IncompleteMatchRows)))
for (Row in 1:length(IncompleteMatchRows)){
    SubsetFrame1[Row,]<-NNPClusterFrame[as.numeric(IncompleteMatchRows[Row]),]
    }
#Create column names for MatchFrame1 to match NNPClusterMatrix
colnames(SubsetFrame1)<-colnames(NNPClusterFrame)

# Split each unit name in units vector into separated words
SplitUnits<-vector("list",length=length(UnitsVector))
for(Unit in 1:length(UnitsVector)){
    SplitUnits[[Unit]]<-unlist(strsplit(UnitsVector[Unit]," "))
    }
    
# Split each character string in SubsetFrame1[,"NNPWords"] into separated words
SplitNNPs1<-vector("list",length=length(SubsetFrame1[,"NNPWords"]))
for(Row in 1:length(SubsetFrame1[,"NNPWords"])){
    SplitNNPs1[[Row]]<-unlist(strsplit(SubsetFrame1[Row,"NNPWords"], " "))
    }

# Search for words from Split Units that match with Split NNPs
MatchWords<-intersect(unlist(SplitNNPs),unlist(SplitUnits))

# Locate the Matches in SplitNNPs
    Matches<-vector("list",length=length(SplitNNPs))
    for(Element in 1:length(SplitNNPs)){
    Matches[[Element]]<-SplitNNPs[[Element]]%in%MatchWords
    }

# Create a match column for SubsetFrame1
MatchColumn<-sapply(Matches,function(x) paste(x,collapse="  "))
SubsetFrame1["Matches"]<-MatchColumn

###################################### Search For General/Common Unit Words ########################################

# Create a dictionary of common unit words
ComUnitWords<-c("Member","Mbr","Formation","Fm","Group","Grp","Supergroup","Strata","Stratum","SprGrp","Spgrp","Unit","Complex","Cmplx","Cplx","Ste","Basement","Pluton","Shale","Alluvium","Amphibolite","Andesite","Anhydrite","Argillite","Arkose","Basalt","Batholith","Bauxite","Breccia","Chert","Clay","Coal","Colluvium","Complex","Conglomerate","Dolerite","Dolomite","Gabbro","Gneiss","Granite","Granodiorite","Graywacke","Gravel","Greenstone","Gypsum","Latite","Marble","Marl","Metadiabase","Migmatite","Monzonite","Mountains","Mudstone", "Limestone","Lm","Ls","Oolite","Ophiolite","Peat","Phosphorite","Phyllite","Pluton","Quartzite","Rhyolite","Salt","Sand","Sands","Sandstone","SS","Sandstones","Schist","Serpentinite","Shale","Silt","Siltstone","Slate","Suite","Sui","Terrane","Till","Tonalite","Tuff","Unit","Volcanic","Volcanics")

# Split each character string in NNPClusterFrame[,"NNPWords"] into separated words
SplitNNPs2<-vector("list",length=length(NNPClusterFrame[,"NNPWords"]))
for(Row in 1:length(NNPClusterFrame[,"NNPWords"])){
    SplitNNPs2[[Row]]<-unlist(strsplit(NNPClusterFrame[Row,"NNPWords"], " "))
    }

MatchWords2<-intersect(unlist(SplitNNPs2),ComUnitWords)

# Locate the Matches in SplitNNPs2
    Matches2<-vector("list",length=length(SplitNNPs2))
    for(Element in 1:length(SplitNNPs2)){
    Matches2[[Element]]<-SplitNNPs2[[Element]]%in%MatchWords2
    }

# Add a column for common unit word matches 
MatchColumn2<-sapply(Matches2,function(x) paste(x,collapse="  "))
NNPClusterFrame["Matches"]<-MatchColumn2

# Use any() function applied to each row of NNPClusterFrame[,"Matches"] to get back rows which have "TRUE"


















 
 ####################################### Find Word Matches #######################

 # Filter UnitsVector so we can search for abbreviated or short forms of Macrostrat unit names
 FilteredUnitsVector<-sapply(UnitsVector,function(x) gsub(" Alluvium| Amphibolite| Andesite| Anhydrite| Argillite| Arkose| Basalt| Basement| Batholith| Bauxite| Breccia| Chert| Clay| Coal| Colluvium| Complex| Conglomerate| Dolerite| Dolomite| Gabbro| Gneiss| Granite| Granodiorite| Graywacke| Gravel| Greenstone| Gypsum| Latite| Marble| Marl| Metadiabase| Migmatite| Monzonite| Mountains| Mudstone| Limestone| Oolite| Ophiolite| Peat| Phosphorite| Phyllite| Pluton| Quartzite| Rhyolite| Salt| Sand| Sands| Sandstone| Sandstones| Schist| Serpentinite| Shale| Silt| Siltstone| Slate| Suite| Terrane| Till| Tonalite| Tuff| Unit| Volcanic| Volcanics","",x))
 
############################## findParents Function (Not Needed) #################################

# write function to find parents of reservoir words in DeepDive documents
findParents<-function(DocRow,FirstDictionary=ReservoirDictionary) {
    CleanedWords<-gsub("\\{|\\}","",DocRow["words"])
    CleanedWords<-gsub("\",\"","COMMASUB",CleanedWords)
    CleanedParents<-gsub("\\{|\\}","",DocRow["dep_parents"])
    SplitParents<-unlist(strsplit(CleanedParents,","))
    SplitWords<-unlist(strsplit(CleanedWords,","))
    FoundWords<-SplitWords%in%FirstDictionary
    
    # Create columns for final matrix
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
  
# Apply findParents function to DeepDiveData to get matched words data
DDResults<-pbapply(DeepDiveData,1,findParents,ReservoirDictionary)
DDResultsMatrix<-do.call(rbind,DDResults)

##################################### Helpful Examples #############################################

# splitting unit names into separate words
Sub<-gsub(" ","SPLIT",UnitDictionary[8819])
SplitUnitsList<-strsplit(noquote(Sub),"SPLIT")
SplitUnits<-unlist(SplitUnitsList)
SplitUnitsList<-vector("list",length=length(UnitDictionary))
unlist(strsplit(noquote(gsub(" ","SPLIT",UnitDictionary[UnitElement])),"SPLIT"))

# Get word position of words in sentence that match FirstWords
SplitWords<-unlist(strsplit(gsub("\\{|\\}","",DeepDiveData[1,"words"]),","))
which(SplitWords%in%FirstWords)

# Look at one sentence from DeepDiveData
CleanedWords<-gsub("\\{|\\}","",DocRow["words"])
SplitWords<-unlist(strsplit(CleanedWords,","))
FoundWords<-SplitWords%in%FirstDictionary
SplitWords<-unlist(strsplit(gsub("\\{|\\}","",DeepDiveData[1,"words"]),","))
Test<-subset(DeepDiveData,DeepDiveData[,"docid"]=="54eb194ee138237cc91518dc"&DeepDiveData[,"sentid"]==796)

#NNP function first draft
NNP<-"NNP"
findNNPs<-function(Sentence, NNP) {
    CleanedSentences<-gsub("\\{|\\}","",Sentence)
    SplitSentences<-strsplit(CleanedSentences,",")
    CleanedPoses<-gsub("\\{|\\}","",Sentence["poses"])
    SplitPoses<-unlist(strsplit(CleanedPoses,","))
    CleanedWords<-gsub("\\{|\\}","",Sentence["words"])
    SplitWords<-unlist(strsplit(CleanedWords,","))
    FoundWords<-SplitWords%in%FirstWords
    MatchedWord<-SplitWords[which(FoundWords)]
    
    NNPs<-which(SplitPoses%in%NNP)
    DocumentID<-rep(Sentence["docid"],length(MatchedWord))
    
    return(cbind(NNPs,DocumentID))
    }
    
# Double check NNPs are being numbered correctly
DDMatches["54eb194ee138237cc91518dc.1",]
DDMatches["54e9e7f8e138237cc91513e9.976",]

# Specific example getting NNP word matches associated with elements in Consecutive Results list vectors for each document
ConsecutiveResults[[1]][3]<-unlist(strsplit(DDMatches[1,"words"],","))[as.numeric(unlist(ConsecutiveResults[[1]][3]))]


#Draft 1 for finding NNP Words associated with consecutive element clusters
# Note: element values for ConsecutiveResults and DDMatches correspond to the same document
matchWords<-function(ConsecutiveResults,DDMatches){
    FinalOutput<-vector("list",length=length(ConsecutiveResults))
    for (Document in 1:length(ConsecutiveResults)) {
        DocumentOutput<-vector("list",length=length(ConsecutiveResults[[Document]]))
        for (NNPCluster in 1:length(ConsecutiveResults[[Document]])) {
            DocumentOutput[[NNPCluster]]<-unlist(strsplit(DDMatches[Document,"words"],","))[as.numeric(unlist(ConsecutiveResults[[Document]][NNPCluster]))]
            }
        FinalOutput[[Document]]<-DocumentOutput
        return(FinalOutput)
        }
 
 # Run function and save list as ConsecutiveNNPWords
 ConsecutiveNNPWords<-matchWords(ConsecutiveResults,DDMatches)
 
 # Save data frame
 write.csv(Object_Name, "Name_Of_Saved_File_Or_Folder.csv")
 
 # Load data frame
 Object_Name = read.csv("Name_Of_Saved_File_Or_Folder.csv", header = TRUE)
