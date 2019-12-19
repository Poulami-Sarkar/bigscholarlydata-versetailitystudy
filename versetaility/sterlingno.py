import common.db as dbcon
import common.config as c
import json
import numpy as np
import pandas as pd
import re 
from os import path

# GLOBAL VARIABLES
thresh =0.1

def calc_versetaility(cur,authors, author_topic, domain):
    sim = np.load('sim_mat/sim_mat.npy')
    data = []
    for auth_id, pub_count,active_years,venue,hindex in authors[:]:
        k = len(author_topic[auth_id])
        if k <2:
            continue
        sno = 0
        div=0
        home_grown  = 0
        x=1
        if auth_id not in author_topic:
            print(auth_id)
            continue
        #print(auth_id, author_topic[auth_id].keys())
        for i,val in author_topic[auth_id].items():
            i_count,prob_sum = val
            for j in list(author_topic[auth_id])[x:]:
                j_count = author_topic[auth_id][j]
                d = 1-sim[int(re.search('\d+',i).group())][int(re.search('\d+',j).group())]
                home_grown += sim[int(re.search('\d+',i).group())][int(re.search('\d+',j).group())]
                div  += (i_count*j_count)/(pub_count**2)
                sno += d*(i_count*j_count)/(pub_count**2)
                #print(i,j)
            x+=1
        
        data.append({
            'author_id': auth_id,
            'sterling_no': sno,
            'home_grown': 1 - (home_grown/(k*(k-1)/2)),
            'diversity' : div,
            'total_topics': len(author_topic[auth_id]),
            'total_publications': pub_count,
            'no_of_paper_venues': venue,
            'total_active_year': active_years,
            'h-index':hindex,
            #'active_years': tuple(set(auth_data[2]))
            
        })
    return data    

def get_author_topics(authors,cur,thresh):
    author_topic = {}
    print(thresh)
    for auth_id, auth_count,active_years,venue,hindex in authors[:]:
        sql = "select topic_id, count(*),sum(prob) from DocTopic_20k_3gram_SE_40 where \
        paper_id in ( \
        select distinct paper_ID paper_id from Paper_Author_Affiliations_SE where author_ID = '"+ auth_id+"') \
        and prob>"+thresh+"group by topic_id;"
        # Initialize no of publications in each topic to 0
        topics = dbcon.dbExecute(cur,sql) 
        topics = [ (top[0],top[1:]) for top in topics ]
        author_topic[auth_id] = dict(topics)
        
    return author_topic

def sterling():
    global thresh
    con = dbcon.dbConnect()
    con,cur = dbcon.dbConnect()
    print('Connection status:',con.is_connected())
    for domain in c.domains:
        #Get list of all authors and details
        sql = 'select pa.author_ID, count(distinct p.paper_ID) count, count(distinct paper_published_year) active_years, count(distinct paper_venue_ID) venue,a.author_h_index from \
            Paper_Author_Affiliations_'+domain+' pa,Papers_'+domain+' p, Authors_'+domain+' a \
	        where p.paper_ID =pa.paper_ID and a.author_ID = pa.author_ID group by pa.author_ID;'

        authors = dbcon.dbExecute(cur,sql)
        file = 'author_topic_'+domain+'_'+str(c.domain_topics[domain])+'.json'
        if not path.exists(file):
            print('Finding topics for %d authors' %len(authors))
            author_topic = get_author_topics(authors,cur,str(thresh))
            with open(file, 'w') as f:
                json.dump(author_topic, f)
                f.close()
        with open(file, 'r') as f:
            author_topic = json.load(f)

        sterling_data = calc_versetaility(cur,authors, author_topic, domain)
        df = pd.DataFrame(sterling_data)
        # Author data on citations
        sql ='select author_ID,sum(cites) from Paper_Author_Affiliations_'+domain+' p, \
            (select paper_ID, count(*) cites from Paper_Citations_'+domain+' group by paper_ID) t \
            where t.paper_ID = p.paper_ID \
            group by author_ID;'
        cites = dbcon.dbExecute(cur,sql)
        cites = pd.DataFrame(cites,columns=['author_id','total_cites'])
        df = df.merge(cites,on='author_id',how='left').fillna(0)
        df[df['total_topics']>0].to_csv('sterling_no_'+domain+'_'+str(c.domain_topics[domain])+'.csv',index=False)
        print(df.corr(method='spearman'))