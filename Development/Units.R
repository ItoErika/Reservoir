UnitsURL<-paste("https://macrostrat.org/api/units?project_id=1&format=csv")
GotURL<-getURL(UnitsURL)
UnitsFrame<-read.csv(text=GotURL,header=TRUE)

StratNamesURL<-paste("https://macrostrat.org/api/defs/strat_names?all&format=csv")
GotURL<-getURL(StratNamesURL)
StratNamesFrame<-read.csv(text=GotURL,header=TRUE)

Units<-merge(x = StratNamesFrame, y = UnitsFrame, by = "strat_name_id", all.x = TRUE)

unit_id, col_id, lat, lng, unit_name, strat_name_long

GoodCols<-c("strat_name_id","strat_name_long","strat_name","unit_id","unit_name","col_id")
Units<-Units[,(names(Units)%in%GoodCols)]


