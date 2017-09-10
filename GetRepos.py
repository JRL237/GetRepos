import boto3
import yaml
import pyodbc
import requests

s3 = boto3.resource("s3", aws_access_key_id="", aws_secret_access_key="")
conn = pyodbc.connect(driver = '{SQL Server Native Client 10.0}', server = 'jrlawsdb.c7lvp9rhyblt.us-east-2.rds.amazonaws.com', database = 'JRL', uid = '', pwd = '', autocommit=True)

def GetRepoMeta(RepoName):
    try:
      r=requests.get("https://raw.githubusercontent.com/byu-oit/%s/master/.repo-meta.yml" % RepoName)
      if(r.ok):
        RepoMeta=yaml.load(r.text)

        SchemaVer="0"
        if("$schemaver" in RepoMeta): SchemaVer=RepoMeta["$schemaver"]

        RepoURL="https://github.com/byu-oit/" + RepoName
        if("repo_url" in RepoMeta): RepoURL=RepoMeta["repo_url"]

        #Check if repository/version already exists in DB
        RepoID=conn.execute("SELECT TOP 1 RepoID FROM Repos WHERE RepoName='%s' AND SchemaVer='%s'" % (RepoName,SchemaVer)).fetchone()
        if(RepoID is not None): 
          print("Not adding %s - already in db" % RepoName)
          return  #Repo in db, so exit

        #Add repository to DB
        RepoID=conn.execute("exec dbo.spAddRepo @RepoName = '%s',@SchemaVer='%s', @RepoURL='%s'" % (RepoName, SchemaVer, RepoURL)).fetchone()[0]
        print("Adding: %s to db\r" % RepoName)
        RepoMetaText =str.replace(r.text,"'","''")

        #Add repo-meta.yml to DB
        conn.execute("exec dbo.spAddRepoMeta @RepoID=%s, @MetaData='%s'" % (RepoID,RepoMetaText))
      else:
        if(r.status_code==404):
          RepoID=conn.execute("exec dbo.spAddRepo @RepoName='%s',@SchemaVer='0', @RepoURL='NOT FOUND'" % RepoName).fetchone()[0]
          print("Adding: %s to db\r" % RepoName)
          conn.execute("exec dbo.spAddRepoMeta @RepoID=%s, @MetaData='NOT FOUND'" % RepoID)

          #Add Repo Link to AWS Bucket
          RepoURL="https://github.com/byu-oit/" + RepoName
          s3.Bucket("jrlreponometa").put_object(Key=RepoName,Body=RepoURL.encode())
        else:
          print(r)
    except Exception as e:
      print("\n\n%s\n\n" % e)

print("\n\nRetrieving Repository Information\n\n")

Page=1

while Page > 0:
    try:
      r=requests.get("https://api.github.com/orgs/byu-oit/repos?per_page=100&page=%s" % (Page))
      if(r.ok):
        RepoInfo=yaml.load(r.text)
        L=len(RepoInfo)
        if L > 0:
          Page += 1
          for i in range(0,L):
            GetRepoMeta(RepoInfo[i]["name"])
        else:
          Page=0  #Reached Last Page - Exit while
      else:
        Page=0  #Request Failed - Exit while
    except Exception as e:
      print("\n\n %s\n\n" %e)
      Page=0  #An exception occured - Exit while
conn.close
print("\n\nFinished\n\n")