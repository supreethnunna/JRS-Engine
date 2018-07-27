import pymongo
import sys
global mystring_skills
mystring_skills = ""


import xml.sax

class ResumeHandler( xml.sax.ContentHandler ):
    def __init__(self):
        global mystring_skills
        #mystring_skills = mystring_skills +'{'
        mystring_skills = mystring_skills +'['
        #mystring_skills = mystring_skills + '{'
        self.NumericValue =""

    # Call when an element starts
    def startElement(self, tag, attributes):
        global mystring_skills
        self.CurrentData = tag
        if tag == "Competency":
            
            #mystring_skills = mystring_skills + '"competency":'
            a = attributes["name"]
            mystring_skills = mystring_skills + '"' + a + '"'
            #mystring_skills = mystring_skills + ','
     
       
        # Call when an elements ends
    def endElement(self, tag):
        global mystring_skills
        if tag == "Competency":
            mystring_skills = mystring_skills + ','
            

    # Call when a character is read
   
if ( __name__ == "__main__"):
    #global mystring_skills
    #mystring_skills = ""
    # create an XMLReader
    parser = xml.sax.make_parser()
    # turn off namepsaces
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)
# override the default ContextHandler
    Handler = ResumeHandler()
    parser.setContentHandler( Handler )
    Test_file = open('C:/Users/Supreeth/Desktop/System_JRS/resume10.xml','r')
    parser.parse(Test_file)
    mystring_skills = mystring_skills[:-1]
    #mystring_skills = mystring_skills + '}'
    mystring_skills = mystring_skills + "]"
    
    
    #mystring_skills = mystring_skills + "}"
    print mystring_skills
    




















