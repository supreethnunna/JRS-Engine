args = commandArgs(trailingOnly=TRUE)

libs = c("tm","plyr","class","e1071","XML");
lapply(libs,require,character.only = TRUE);

workdir <-"C:/xampp/htdocs/JRS";
stringsAsFactors=FALSE;
setwd(workdir);
options(stringsAsFactors = FALSE );
########################################################
similarity<-"Cosine";
method <-"TF-IDF";
if (length(args)==0) {
  print("Stopping app....")
  stop("Working directory must be supplied as argument..", call.=FALSE)
  
} else if (length(args)>=1) {
  
  similarity= args[1];
  method = args[2];
}
###############################################################################

resu_sample = readChar("desc.txt",nchars=1e6)
resu_data = read.csv("Resume.csv",header = TRUE);
jobDesc_data = read.csv("job_descUTF8.csv", header = TRUE);
resu_data= data.frame(cbind("id","title",resu_sample))
colnames(resu_data) <- c("id","role","Descr")
#select_resume = 1
job_listings = nrow(jobDesc_data)
#resume_category = resu_data[select_resume,2]
#View(resu_data[a,])
colnames(resu_data) <- c("id","role","Descr")
colnames(jobDesc_data) <- c("id","role","Descr")
append = rbind(resu_data,jobDesc_data)
#View(jobDesc_data)
#View(resu_data)
#View(append)
#################################################################################
#clean text
cleancorpus = function(corpus){
  #mycorpus = corpus;
  corpus.tmp = tm_map(corpus,removePunctuation);
  corpus.tmp = tm_map(corpus.tmp,stripWhitespace);
  corpus.tmp = tm_map(corpus.tmp,removeNumbers);
  corpus.tmp = tm_map(corpus.tmp,tolower);
  myStopWords = c(stopwords("english"), "why","how","when","who","which","via","one","two","monday","tuesday","wednesday","thursday","friday","saturday","sunday","morning","afternoon","evening","night","white","will","within","using","used","use","well");
  corpus.tmp = tm_map(corpus.tmp,removeWords,myStopWords);
  corpus.tmp <- tm_map(corpus.tmp, stemDocument);
  #corpus.tmp = tm_map(corpus.tmp,stemCompletion,dictionary = mycorpus);
  corpus.tmp <- tm_map(corpus.tmp, PlainTextDocument);
  return(corpus.tmp);
}

#################################################################################
#create TDM
generateDTM = function(description)
{
  #s.cor.tmp = subset(data,IncidentType_category == label,select=c(Description));
  
  s.cor = Corpus(DataframeSource(description));
  s.cor.cl = cleancorpus(s.cor);
  #s.tdm = TermDocumentMatrix(s.cor.cl);
  s.dtm = DocumentTermMatrix(s.cor.cl, control=list(weighing=weightTfIdf, minWordLength=1, minDocFreq=1));
  s.dtm = removeSparseTerms(s.dtm,0.95);
  
  #return = list(name = labels,dtm = s.dtm);
  return= s.dtm;
}
#################################################################################
description<- data.frame()
for(i in 1:nrow(append)){
  description[i,1]<- append[i,3];
}  

dtm = generateDTM(description)
dtm = as.matrix(dtm)
############################################################################
myscore = dim(job_listings)
resume_tfidf = dtm[1,]
Jobs_tfidf = dtm[2:nrow(dtm),]

##################################################################################

JaccardScores <- function(mymat)
{
  for (i in 1:nrow(Jobs_tfidf)){
    myscore[i]= sum(resume_tfidf*Jobs_tfidf[i,])/((sum(resume_tfidf^2)) + (sum(Jobs_tfidf[i,]^2)) - sum(resume_tfidf*Jobs_tfidf[i,]))
  }
  return(myscore)
}

CosineScores <- function(mymat){
  for (i in 1:nrow(Jobs_tfidf)){
    myscore[i]= sum(resume_tfidf*Jobs_tfidf[i,])/sqrt(sum(resume_tfidf^2)*sum(Jobs_tfidf[i,]^2))
  }
  return(myscore)
}

EuclidianScores <- function(mymat){
  for (i in 1:nrow(Jobs_tfidf)){
    myscore[i]= sqrt(sum(resume_tfidf - Jobs_tfidf[i,])^2)
  }
  return(myscore)
}

scoring_Type <- function(type,mymat){
  if (type == "Cosine"){
    scores = CosineScores(mymat)
    return(scores)
  } else if (type == "Euclidian")  {
    scores = EuclidianScores(mymat)
    return(scores)
  } else if (type == "Jaccard") {
    scores = JaccardScores(mymat)
    return(scores)
  } else {
    return("error")
  }
}

#########################################################################################
scores = scoring_Type(similarity,dtm)
#roles = jobDesc_data[,c(1,2)]
roles = jobDesc_data
dataframe_role_score = cbind(roles,scores)
dataframe_role_score = data.frame(dataframe_role_score[order(-scores),])
dataframe_role_score = dataframe_role_score[,c(2,3,4)]
##############################################################################
#dataframe_role_score = toString(dataframe_role_score)
#dataframe_role_score = enc2utf8(dataframe_role_score)
#dataframe_role_score
#View(dataframe_role_score)
#head(dataframe_role_score)

library(XML)
convertToXML <- function(df,name)
{
  xml <- xmlTree("Test")
  xml$addNode(name, close=FALSE)
  for (i in 1:nrow(df)) {
    xml$addNode("value", close=FALSE)
    for (j in names(df)) {
      xml$addNode(j, df[i, j])
    }
    xml$closeTag()
  }
  xml$closeTag()
  return(xml)
}
tr = convertToXML(dataframe_role_score,"Tabelle") 
xmlstring = saveXML(tr$value(),file="mydata.xml") ## looks good
cat(saveXML(tr$value()))



#library(kulife)
#dataframe_role_score = data.frame(dataframe_role_score)
#View(dataframe_role_score)
#write.xml(dataframe_role_score, file="mydata.xml")
## Looks like it is no problem of the function, because the following line
# shows the same error
#colnames(dataframe_role_score)
#?colnames
