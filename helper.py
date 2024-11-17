from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from IPython.display import display, Markdown
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import numpy as np
from scipy import stats
import scipy

spark = SparkSession.builder \
    .appName("NS") \
    .master("local[*]") \
    .getOrCreate()

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

def local_ownership(df, df100mn):
    my_list_sql = tuple(df100mn.rdd.flatMap(lambda x: x).collect())
    for year in ["2024"]:
        display(Markdown(f"**{year}**"))
        ydf = df.select("src", "dst").filter((F.col(year).isNotNull()) & (F.col(year) != "0") & (F.trim(F.col(year)) != "") & (F.col("INVESTOR DOMICILE") == "Local"))
        ydf.createOrReplaceTempView("papers")
    
        query = f"""
        SELECT DISTINCT  
                p1.dst AS src, 
                p2.dst AS dst
        FROM papers p1
        JOIN papers p2
        ON p1.src = p2.src
        WHERE p1.dst != p2.dst
        AND (p1.dst IN {my_list_sql} OR p2.dst IN {my_list_sql})
        """
    
        result_df = spark.sql(query)
    
        G = nx.Graph()
        G.add_edges_from(result_df.rdd.map(tuple).collect())
        
        pos = nx.fruchterman_reingold_layout(G, seed=42)
        
        connected_components = list(nx.connected_components(G))
    
        plt.figure(figsize=(18, 18))
        for idx, component in enumerate(connected_components):
            # Get the nodes in each component
            component_nodes = list(component)
            component_label = ',\n'.join(str(node) for node in component_nodes[:30])+",\n..."
            
            # Draw nodes with a specific color per component
            nx.draw_networkx_nodes(
                G, pos, nodelist=component_nodes, 
                node_size=700, 
                node_color=plt.cm.tab10(idx / len(connected_components)), 
                label=component_label
            )
            # Add labels for each node in the component
            labels = {node: f'{node}' for node in component_nodes}
        plt.legend(scatterpoints=1, loc="lower left", fontsize=9, markerscale=0.5, bbox_to_anchor=(-0.21, 0), ncol=3)
        plt.axis("off")
        plt.show()

def pdc():
    df_pdc = spark.read.csv("data/PDC-Network.csv", header=True, inferSchema=True)
    df_pdc = df_pdc.withColumn("STOCK", F.regexp_replace(F.col("STOCK"), " PM Equity$", ""))
    df_pdc = df_pdc.withColumnRenamed("STOCK", "src").withColumnRenamed("HOLDER NAME", "dst")
    ydf = df_pdc.select("src", "dst")
    
    edges = ydf.select("src", "dst")
    vertices = ydf.select("dst").distinct()
    vertices2 = ydf.select("src").distinct()
    
    G = nx.Graph()
    G.add_nodes_from(vertices.rdd.flatMap(lambda x: x).collect(), bipartite=0)
    G.add_nodes_from(vertices2.rdd.flatMap(lambda x: x).collect(), bipartite=1)
    G.add_edges_from(edges.rdd.map(tuple).collect())
    
    top_nodes, bottom_nodes = nx.bipartite.sets(G)
    
    pos = nx.fruchterman_reingold_layout(G, seed=42)
    
    fig, axs = plt.subplots(1, 2, figsize=(20,10))
    
    nx.draw_networkx_nodes(G, pos, ax=axs[0], nodelist=top_nodes, node_size=20, node_color="red", label="Holders")
    nx.draw_networkx_nodes(G, pos, ax=axs[0], nodelist=bottom_nodes, node_size=20, node_color="black", label="Stock")
    nx.draw_networkx_edges(G, pos, ax=axs[0], alpha=0.4)
    
    axs[0].axis("off") 
    axs[0].set_title("Bipartite View")
    
    projected_V = nx.bipartite.projected_graph(G, vertices.rdd.flatMap(lambda x: x).collect())
    
    pos = nx.fruchterman_reingold_layout(projected_V, seed=42)
    
    nx.draw_networkx_nodes(projected_V, pos, ax=axs[1], node_size=30, node_color="red", label="Holders")
    nx.draw_networkx_edges(projected_V, pos, ax=axs[1], alpha=0.4)
    axs[1].set_title("Holder-Holder Projection")
    axs[1].axis("off")
    
    centrality_methods = [
        (nx.degree_centrality, "degree"),
        (nx.betweenness_centrality, "betweenness"),
        (nx.closeness_centrality, "closeness"),
        (nx.pagerank, "pagerank"),
    ]
    
    # List to store individual DataFrames
    centrality_dfs = []
    
    for method, name in centrality_methods:
        cent_ = method(projected_V)
        top = sorted(cent_.items(), key=lambda item: item[1], reverse=True)[:5]
        centrality_dfs.append(pd.DataFrame(top, columns=['id', f'{name} Centrality']))
    
    all_centralities = pd.concat(centrality_dfs, axis=1)
    plt.show()
    display(all_centralities)
    summary(projected_V)

def prep_df1():
    df = spark.read.csv("data/Index-Holders.csv", header=True, inferSchema=True)\
        .drop("_c1", "PORTFOLIO NAME") \
        .withColumn("STOCK", F.regexp_replace(F.col("STOCK"), " PM Equity$", "")) \
        .withColumnRenamed("STOCK", "src") \
        .withColumnRenamed("HOLDER NAME", "dst")

    df100mn = spark.read.csv("data/Holders-100mln.csv", header=True, inferSchema=True)
    
    return df

def prep_df2():
    df = spark.read.csv("data/Index-Holders_Domicile.csv", header=True, inferSchema=True) \
    .drop("_c1", "PORTFOLIO NAME") \
    .withColumn("STOCK", F.regexp_replace(F.col("STOCK"), " PM Equity$", "")) \
    .withColumnRenamed("STOCK", "src") \
    .withColumnRenamed("HOLDER NAME", "dst")

    df100mn = spark.read.csv("data/Holders-100mln.csv", header=True, inferSchema=True)
    
    return df, df100mn