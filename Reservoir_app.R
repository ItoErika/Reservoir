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

if (require("RPostgreSQL",warn.conflicts=FALSE)==FALSE) {
    install.packages("RPostgreSQL",repos="http://cran.cnr.berkeley.edu/");
    library("data.table");
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

# Remove symbols 
DeepDiveData[,"words"]<-gsub("\\{|\\}","",DeepDiveData[,"words"])
# Make a substitute for commas so they are counted correctly as elements for future functions
DeepDiveData[,"words"]<-gsub("\",\"","COMMASUB",DeepDiveData[,"words"])
# Remove commas from DeepDiveData
CleanedWords<-gsub(","," ",DeepDiveData[,"words"])

# Download geologic unit names from Macrostrat Database
# download unit_name data from Macrostrat
UnitsURL<-paste("https://dev.macrostrat.org/api/units?project_id=1&format=csv&response=long")
GotURL<-getURL(UnitsURL)
UnitsFrame<-read.csv(text=GotURL,header=TRUE)

#download strat_name_long data from Macrostrat
StratNamesURL<-paste("https://dev.macrostrat.org/api/defs/strat_names?all&format=csv")
GotURL<-getURL(StratNamesURL)
StratNamesFrame<-read.csv(text=GotURL,header=TRUE)

# Get rid of columns from UnitsFrame that are also in StratNamesFrame so the merge function can run smoothly EXCEPT "strat_name_id"
UnitsFrameNoOverlap<-UnitsFrame[,!(names(UnitsFrame) %in% names(StratNamesFrame))]
UnitsFrame<-UnitsFrame[,c(colnames(UnitsFrameNoOverlap),"strat_name_id")]

# Merge the data from UnitsFrame to StratNamesFrame by strat_name_id
Units<-merge(x = StratNamesFrame, y = UnitsFrame, by = "strat_name_id", all.x = TRUE)
Units<-Units[,c("strat_name_id","strat_name_long","strat_name","unit_id","unit_name","col_id","t_age","b_age","max_thick","min_thick","lith")]
Units<-na.omit(Units)

# Add a column of mean thickness to Units 
Units$mean_thick<-apply(Units,1,function(row) mean(row["max_thick"]:row["min_thick"]))

# Make two dictionaries: one of long strat names and one of short strat names
# make a matrix of long and short units only. 
LongShortUnits<-Units[,c("strat_name_long","strat_name")]
# remove duplicate rows
LongShortUnits<-unique(LongShortUnits)

# Create dictionaries
LongUnitDictionary<-as.character(LongShortUnits[,"strat_name_long"])
ShortUnitDictionary<-as.character(LongShortUnits[,"strat_name"])

# Search for the long unit names in CleanedWords
Start<-print(Sys.time())
LongUnitHits<-parSapply(Cluster,LongUnitDictionary,function(x,y) grep(x,y,ignore.case=FALSE, perl = TRUE),CleanedWords)
End<-print(Sys.time())

# Create a matrix of unit hits locations with corresponding unit names
LongUnitHitsLength<-pbsapply(LongUnitHits,length)
LongUnitNames<-rep(names(LongUnitHits),times=LongUnitHitsLength)
LongUnitData<-cbind(LongUnitNames,unlist(LongUnitHits))
# convert matrix to data frame
LongUnitData<-as.data.frame(LongUnitData)
# convert row match location (denotes location in CleanedWords) column to numerical data
colnames(LongUnitData)[2]<-"MatchLocation"
LongUnitData[,"MatchLocation"]<-as.numeric(as.character(LongUnitData[,"MatchLocation"]))

# Remove MatchLocation rows which appear more than once (remove sentences in which more than one unit occurs)
# Make a table showing the number of unit names which occur in each DeepDiveData row that we know has at least one unit match
RowHitsTable<-table(LongUnitData[,"MatchLocation"])
# Locate and extract rows which contain only one long unit
# Remember that the names of RowHitsTable correspond to rows within CleanedWords
SingleHits<-as.numeric(names(RowHitsTable)[which((RowHitsTable)==1)])    
# Subset LongUnitData to get dataframe of Cleaned Words rows and associated single hit long unit names
SingleHitData<-subset(LongUnitData,LongUnitData[,"MatchLocation"]%in%SingleHits==TRUE)
# Create a column of sentences from CleanedWords and bind it to SingleHitData
Sentence<-CleanedWords[SingleHitData[,"MatchLocation"]]
SingleHitData<-cbind(SingleHitData,Sentence)   

# Locate documents in which matches occurred for each respective strat_name_long
# Unlist the row location data for each element(unit) in LongUnitHits
MatchRowList<-lapply(LongUnitHits,function(x) unlist(x))
# Extract docid data associated with each row in MatchRowList
# Remember the rows in DeepDiveData match the rows in CleanedWords
MatchDocList<-lapply(MatchRowList,function(x) DeepDiveData[x,"docid"])
    
# Search for short unit names in CleanedWords
Start<-print(Sys.time())
ShortUnitHits<-parSapply(Cluster,ShortUnitDictionary,function(x,y) grep(x,y,ignore.case=FALSE, perl = TRUE),CleanedWords)
End<-print(Sys.time())

# Run a search on documents which contain long unit names (MatchDocList)
# Use a function to locate sentences within those documents which contain the word "aquifer" and a short unit name.
# Note that this does not sub out spaces for commas, because we are doing a single word search    
findPairs<-function(DeepDiveData,MatchDocList,ShortUnitHits,Word="aquifer") {
    FinalVector<-vector("logical",length=length(MatchDocList))
    names(FinalVector)<-names(MatchDocList)
    for (i in 1:length(FinalVector)) {
        Temp<-DeepDiveData[ShortUnitHits[[i]],]
        DocSubset<-subset(Temp,Temp[,"docid"]%in%MatchDocList[[i]]==TRUE)
        FinalVector[i]<-length(grep(Word,DocSubset[,"words"],ignore.case=TRUE))
        setTxtProgressBar(progbar,i)
        }
    return(FinalVector)
    }
    
Matches<-findPairs(DeepDiveData[,c("docid","words")],MatchDocList,ShortUnitHits,"aquifer")    
    
