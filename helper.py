from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from IPython.display import display, Markdown
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import numpy as np
from scipy import stats
import scipy


def summary(Nx):
    N, L = Nx.order(), Nx.size()
    avg_deg = 2 * (float(L) / N)

    data = {
        'Metric': ['Nodes', 'Edges'],
        'Value': [N, L,]
    }

    df_1 = pd.DataFrame(data).round(3)
    display(Markdown("**Network Statistics**"))
    display(df_1)

    degrees = [k for node, k in nx.degree(Nx)]
    k_min = np.min(degrees)
    k_max = np.max(degrees)
    k_avg = np.mean(degrees)

    degree_data = {
        'Metric': ['Minimum Degree', 'Maximum Degree', 'Average Degree'],
        'Value': [k_min, k_max, k_avg]
    }

    df_2 = pd.DataFrame(degree_data).round(3)
    display(Markdown("**Degree Distribution**"))
    display(df_2)

    plt.figure(figsize=(2.5, 2.5))
    ax = plt.subplot(1,1,1)

    degrees = [k for node, k in nx.degree(Nx)]
    
    _, bins, _ = ax.hist(degrees, bins=15, density=True, label="data");
    bin_middles = 0.5 * (bins[1:] + bins[:-1]);
    mu, sigma = scipy.stats.norm.fit(degrees)
    best_fit_line = scipy.stats.norm.pdf(bins, mu, sigma)
    
    ax.plot(bins, best_fit_line, lw=3, label="fit");
    ax.set_ylabel("Freuency (p)")
    ax.set_xlabel('Degree (k)')
    plt.legend();
    plt.show()
    
    display(Markdown("**Shortest Paths**"))
    
    try:
        print(f"Average shortest path length: {round(nx.average_shortest_path_length(Nx), 3)}")
    except:
        print("Network is disconnected")
        GC_nodes = max(nx.connected_components(Nx), key=len)
        GC = G.subgraph(GC_nodes).copy()
        print(f"Average shortest path length of GC: {round(nx.average_shortest_path_length(GC), 3)}")

    display(Markdown("**Clustering Coefficient**"))
    cc = nx.clustering(Nx)
    df_clustering = pd.DataFrame(list(cc.items()), columns=['Node', 'Clustering Coefficient'])

    # Set the Node as the index
    df_clustering.set_index('Node', inplace=True)
    avg_clust = sum(cc.values()) / len(cc)
    print(f"average clustering coefficient: {round(avg_clust, 3)}")

def make_bip_net_graph(df):
    fig, axes = plt.subplots(2, 3, figsize=(21, 14))
    plt.close(fig);

    years = ["2019", "2020", "2021", "2022", "2023", "2024"]
    for ax, year in zip(axes.flat, years):
        ax.set_title(year)
        ax.axis("off")
        ydf = df.select("src", "dst").filter((F.col(year).isNotNull()) & (F.col(year) != "0") & (F.trim(F.col(year)) != ""))
    
        edges = ydf.select("src", "dst")
        vertices = ydf.select("dst").distinct()
        vertices2 = ydf.select("src").distinct()
    
        G = nx.Graph()
        G.add_nodes_from(vertices.rdd.flatMap(lambda x: x).collect(), bipartite=0)
        G.add_nodes_from(vertices2.rdd.flatMap(lambda x: x).collect(), bipartite=1)
        G.add_edges_from(edges.rdd.map(tuple).collect())
        
        top_nodes, bottom_nodes = nx.bipartite.sets(G)
        
        pos = nx.spring_layout(G, seed=42)
    
        nx.draw_networkx_nodes(G, pos, nodelist=top_nodes, node_size=30, node_color="red", label="Holders", ax=ax)
        nx.draw_networkx_nodes(G, pos, nodelist=bottom_nodes, node_size=600, node_color="black", label="Stock", ax=ax)
        nx.draw_networkx_edges(G, pos, alpha=0.4, ax=ax)
        nx.draw_networkx_labels(G, pos, labels={node: node for node in bottom_nodes}, font_size=10, font_color="white", ax=ax)
        ax.legend(scatterpoints=1, loc="upper left", fontsize=8, markerscale=0.5)
    return fig

def final_summary(df):
    for year in ["2024"]:
        display(Markdown(f"**{year} - Holder-Holder - Projection**"))
        ydf = df.select("src", "dst").filter((F.col(year).isNotNull()) & (F.col(year) != "0") & (F.trim(F.col(year)) != ""))
    
        edges = ydf.select("src", "dst")
        vertices = ydf.select("dst").distinct()
        vertices2 = ydf.select("src").distinct()
    
        G = nx.Graph()
        G.add_nodes_from(vertices.rdd.flatMap(lambda x: x).collect(), bipartite=0)
        G.add_nodes_from(vertices2.rdd.flatMap(lambda x: x).collect(), bipartite=1)
        G.add_edges_from(edges.rdd.map(tuple).collect())
        
        # Separate nodes by their bipartite attribute
        top_nodes, bottom_nodes = nx.bipartite.sets(G)
    
        projected_V = nx.bipartite.projected_graph(G, vertices.rdd.flatMap(lambda x: x).collect())
        summary(projected_V)
      
        centrality_methods = [
            (nx.degree_centrality, "degree"),
            (nx.betweenness_centrality, "betweenness"),
            (nx.closeness_centrality, "closeness"),
            (nx.pagerank, "pagerank"),
        ]
        display(Markdown(f"**Centralities**"))
        for methods_, name in centrality_methods:
            cent_ = methods_(projected_V)
            top = sorted(cent_.items(), key=lambda item: item[1], reverse=True)[:3]
            display(pd.DataFrame(top, columns=['id', f'{name} Centrality']))

def prep_df():
    spark = SparkSession.builder \
        .appName("NS") \
        .master("local[*]") \
        .getOrCreate()
    df = spark.read.csv("data/Index-Holders.csv", header=True, inferSchema=True)\
        .drop("_c1", "PORTFOLIO NAME") \
        .withColumn("STOCK", F.regexp_replace(F.col("STOCK"), " PM Equity$", "")) \
        .withColumnRenamed("STOCK", "src") \
        .withColumnRenamed("HOLDER NAME", "dst")
    return df