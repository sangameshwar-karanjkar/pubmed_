from concurrent.futures import ProcessPoolExecutor,ThreadPoolExecutor
from google.cloud import bigquery
from google.cloud import pubsub_v1
from lxml import html
import urllib.request
import pandas as pd
import datetime
import os
import json

def uploaded_file():
    bigquery_client = bigquery.Client()
    query = """SELECT distinct filename FROM `springer-nature-analytics.pubmed.pubmed_full_refresh`"""

    query_job = bigquery_client.query(query)
    uploaded_files = []
    for row1 in query_job:
        deta_ = dict(row1)
        uploaded_files.append(str(deta_['filename']))
    return uploaded_files

def parse_xml(record):

    json_data={"MedlineCitation":{"Status":None,"Owner":None,"PMID":{"Version":None,"text":None},"DateRevised":{"Year":None,"Month":None,"Day":None},
                                  "Article":{"Journal":{"JournalIssue":{"PubDate":{"Year":None,"Month":None,"Day":None}}}}},
               "PubmedData":{"History":{"PubMedPubDate":[]},"ArticleIdList":{"ArticleId":[]}},
               "filename":file_name,"last_updated":str(datetime.date.today()),"flag":'base_2023'}

    elemnt = html.fromstring(record)

    Medline=['Status','Owner']
    for Medline_ in Medline:
        try:
            json_data['MedlineCitation'][Medline_] = elemnt.xpath('.//medlinecitation/@'+Medline_.lower())[0]
        except:
            pass
    try:
        json_data['MedlineCitation']['PMID']['Version'] = int(elemnt.xpath('.//medlinecitation/pmid/@version')[0])
    except:
        pass
    try:
        json_data['MedlineCitation']['PMID']['text'] = str(elemnt.xpath('.//medlinecitation/pmid/text()')[0])
    except:
        pass
    DateRevi =["Year","Month","Day"]
    for DateRevi_ in DateRevi:
        try:
            json_data['MedlineCitation']['DateRevised'][DateRevi_] = int(elemnt.xpath('.//medlinecitation/daterevised/'+DateRevi_.lower()+'/text()')[0])
        except:
            pass
    PubDate =["Year","Month","Day"]
    for PubDate_ in PubDate:
        try:
            json_data['MedlineCitation']['Article']['Journal']['JournalIssue']['PubDate'][PubDate_] = \
                str(elemnt.xpath('.//medlinecitation/article/journal/journalissue/pubdate/' + PubDate_.lower()+'/text()')[0])
        except:
            pass
    PubmedData= elemnt.xpath('.//pubmeddata/history/pubmedpubdate')
    if len(PubmedData) != 0:
        for PubmedData_ in PubmedData:
            pubdata = {"PubStatus": None, "Year": None, "Month": None, "Day": None}
            pubs = ["PubStatus", "Year", "Month", "Day"]
            for pubs_ in pubs:
                try:
                    pubdata[pubs_] = PubmedData_.xpath('.//@' + pubs_.lower())[0]
                except:
                    try:
                        pubdata[pubs_] = int(PubmedData_.xpath('.//' + pubs_.lower()+'/text()')[0])
                    except:
                        pass
            json_data['PubmedData']['History']['PubMedPubDate'].append(pubdata)
    else:
        pubdata = {"PubStatus": None, "Year": None, "Month": None, "Day": None}
        json_data['PubmedData']['History']['PubMedPubDate'].append(pubdata)

    ArticleIdList = elemnt.xpath('.//pubmeddata/articleidlist/articleid')
    if len(ArticleIdList) != 0:
        for ArticleIdList_ in ArticleIdList:
            ArticleId = {"IdType": None, "text": None}
            try:
                ArticleId['IdType'] = str(ArticleIdList_.xpath('.//@idtype')[0])
            except:
                pass
            try:
                ArticleId['text'] = str(ArticleIdList_.xpath('.//text()')[0])
            except:
                pass
            json_data['PubmedData']['ArticleIdList']['ArticleId'].append(ArticleId)
    else:
        ArticleId = {"IdType": None, "text": None}
        json_data['PubmedData']['ArticleIdList']['ArticleId'].append(ArticleId)

    try:
        publish_future = publisher_pub.publish(topic_path, data=json.dumps(json_data).encode('utf-8'))
        publish_future.result()
    except Exception as e:
        print(e)
        print("Gatting pub-sub error")





if __name__=="__main__":
    # https: // eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=1

    publisher_pub = pubsub_v1.PublisherClient()
    topic_path = 'projects/springer-nature-analytics/topics/pubmed_to_bigquery'

    links =['ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/']
    #links = ['ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/']
    for link in links:
        with urllib.request.urlopen(link) as k:
            html_ = k.read()
            with open("pubmed_daily_update.csv", "wb") as f:
                f.write(html_)
        df=pd.read_csv("pubmed_daily_update.csv",header=None)[0].str.split(expand=True)
        files = df[8].tolist()
        files=[element for element in files if '.md5' not in element and '.xml.gz' in element]
        uploaded_files = uploaded_file()
        new_files=[]
        for file in files:
            if str(file).replace('.gz','').lower() not in str(uploaded_files).replace('.gz','').lower():
                new_files.append(file)
        os.system("rm -r datas")
        os.system("mkdir datas")
        file_count=0
        for new_file in new_files:
            file_count +=1
            os.system("echo " + str(file_count))
            os.system("wget -q -P /datas/ "+link+new_file)
            os.system("gunzip /datas/"+new_file)

            file_name = str(new_file).replace('.gz','')
            os.system("echo " + file_name)

            data = html.parse('/datas/' + file_name)
            recoeds = data.xpath('.//pubmedarticle')
            string_list = [bytes(html.tostring(recoed)) for recoed in recoeds]
            data=''
            recoeds=''
            os.system("echo uploading started")
            #for string_list_ in string_list:
               # parse_xml(string_list_)

            with ThreadPoolExecutor(max_workers=5) as exe:
                result = exe.map(parse_xml, string_list)

            os.system('rm ' + '/datas/' + file_name)
        os.system("rm pubmed_daily_update.csv")