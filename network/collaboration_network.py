import numpy as np
import networkx as nx
import common.config as c
import common.db as dbcon
import pandas as pd
import matplotlib.pyplot as plt
import time
from os import path
from versetaility.sterlingno import sterling 

def get_graph_metrics(authors,graph):
    G = nx.Graph()
    G.add_nodes_from(authors)
    edges = list(zip(graph['auth1'],graph['auth2'],graph['count']))
    G.add_weighted_edges_from(edges,weight="wts")

    print("Calculating degree")    
    #deg = G.degree(weight='wts')
    deg = G.degree()
    df = pd.DataFrame(deg, columns=['author_id', 'degree'])
    #df = df.merge(temp,on=('author_id'))
    df.to_csv('graph_metrics.csv',index=False) 

    #page_rank = nx.pagerank(G,weight='wts')
    page_rank = nx.pagerank(G)
    temp = pd.DataFrame(page_rank.items(), columns=['author_id', 'pagerank'])
    df = df.merge(temp,on=('author_id'))
    df.to_csv('graph_metrics.csv',index=False)

    print("Calculating closeness centrality")
    #close_c = nx.closeness_centrality(G,distance='wts')
    close_c = nx.closeness_centrality(G)
    temp = pd.DataFrame(close_c.items(), columns=['author_id', 'closeness_cent'])
    df = df.merge(temp,on=('author_id'))
    df.to_csv('graph_metrics.csv',index=False)

    print("Calculating eigenvalue centrality")
    #eigen_c = nx.eigenvector_centrality(G,weight='wts')
    eigen_c = nx.eigenvector_centrality(G)
    temp = pd.DataFrame(eigen_c.items(), columns=['author_id', 'eigenvector_cent'])
    df = df.merge(temp,on=('author_id'))
    df.to_csv('graph_metrics.csv',index=False)

    print("neighbourhood degree")
    #nei_deg = nx.average_neighbor_degree(G,weight='wts')
    nei_deg = nx.average_neighbor_degree(G)
    temp = pd.DataFrame(nei_deg.items(), columns=['author_id', 'avg_neighbor_deg'])
    df = df.merge(temp,on=('author_id'))
    df.to_csv('graph_metrics.csv',index=False)

    print("Calculating betweenness centrality")
    #bc = nx.betweenness_centrality(G, k=None, weight='wts')
    bc = nx.betweenness_centrality(G, k=None)
    temp = pd.DataFrame(bc.items(), columns=['author_id', 'betweenness_cent'])
    df = df.merge(temp,on=('author_id'))
    df.to_csv('graph_metrics.csv',index=False)
    return df
    

def generate_network():

    for domain in c.domains:
        file = 'sterling_no_'+domain+'_'+str(c.domain_topics[domain])+'.csv'
        if not path.exists(file):
            print("Generating topics ans sterling numbers")
            sterling()
        df = pd.read_csv(file)
        authors = np.array(df['author_id'])
        print(len(authors))

        # Get graph edges
        con = dbcon.dbConnect()
        con,cur = dbcon.dbConnect()
        print('Connection status:',con.is_connected())
        sql = "SELECT p1.paper_ID, p1.author_ID a1, p2.author_ID a2 \
            FROM Paper_Author_Affiliations_SE p1 \
            INNER JOIN Paper_Author_Affiliations_SE p2 ON p1.paper_ID = p2.paper_ID where p1.author_ID <> p2.author_ID;"
        graph = dbcon.dbExecute(cur,sql)
        graph = pd.DataFrame(graph, columns =['paper_ID', 'auth1', 'auth2']) 
        
        # Remove authors with topics < 1
        graph = graph[graph.auth1.isin(authors) & graph.auth2.isin(authors)]
        
        # Create edges
        graph = graph.groupby(by=['auth1','auth2']).size().reset_index(name='count')
        graph['edges'] = list(zip(graph['auth1'],graph['auth2']))
        graph[['edges']] = graph['edges'].apply(sorted)
        graph[['edges']] = graph['edges'].apply(tuple)

        # For undirected graph drop dupilcate edges
        undir_graph = graph.drop_duplicates(subset='edges',keep='first')
        print(len(graph),len(undir_graph))
        print(undir_graph.head())
        print("Creating graph")
        metrics = get_graph_metrics(authors,undir_graph)
        df = df.merge(metrics,on=('author_id'))
        df.to_csv('dataframe_'+domain+'_'+str(c.domain_topics[domain])+'.csv',index=False)