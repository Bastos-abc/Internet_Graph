#!/usr/bin/python3
import os
import pickle
import pandas as pd
import networkx as nx
# https://networkx.org/documentation/latest/auto_examples/index.html
# needs scipy
# needs pygraphviz
# https://pygraphviz.github.io/documentation/stable/install.html
from multiprocessing import Pool
from matplotlib import pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.backends.backend_pdf import PdfPages
from config import out_folder, img_folder, n_threads, initial_date, days



def node_position(vect=[], line=0, max_col=15):
    col = 0
    pos = {}
    alpha = {}
    if len(vect) > max_col:
        control = 1
    elif len(vect) > 1:
        control = max_col / (len(vect))
    else:
        control = max_col / 2
        col = control
    for n, a in vect:
        pos[n] = (col, line)
        alpha[n] = a
        col += control
        if col >= max_col:
            line += 1
            col = 0
    return pos, alpha, line


def create_graph_plot(asn= int, link={}, total_days = 1):
    print("Preparing information to plotting graph and table from AS", asn)
    if type(link) is str:
        file = link
        if os.path.isfile():
            try:
                f = open(file,'rb')
                link = pickle.load(f)
                f.close()
            except:
                print('Fail to import information from',file)
                exit(1)
    if not asn in link.keys():
        print("ASN", asn, "not found on links information")
        return None
    if not os.path.isdir(img_folder):
        os.mkdir(img_folder)
    neighbors = link[asn]['neighbors'].keys()
    connections = []
    providers = []
    customers = []
    peers = []
    ixp = []
    sibling = []
    providers_i = []
    customers_i = []
    peers_i = []
    sibling_i = []
    ixp_i = []
    for i, n in enumerate(neighbors):
        visible_days = max([int(link[asn]['neighbors'][n]['days_right']), int(link[asn]['neighbors'][n]['days_left'])])
        visible_days = visible_days / total_days
        if (link[asn]['neighbors'][n]['relationship'] == -1 # provider from CAIDA
                or link[asn]['neighbors'][n]['relationship'] == -2): # provable provider
            if link[asn]['neighbors'][n]['Active']:
                providers.append([n,visible_days])
            else:
                providers_i.append([n,visible_days])
            connections.append((n, asn))
        elif (link[asn]['neighbors'][n]['relationship'] == 1 or # customer from CAIDA
            link[asn]['neighbors'][n]['relationship'] == 2): # provable customer
            if link[asn]['neighbors'][n]['Active']:
                customers.append([n,visible_days])
            else:
                customers_i.append([n,visible_days])
            connections.append((n, asn))
        elif link[asn]['neighbors'][n]['relationship'] == 0: # peer from CAIDA
            if link[asn]['neighbors'][n]['Active']:
                peers.append([n,visible_days])
            else:
                peers_i.append([n,visible_days])
            connections.append((n, asn))
        elif link[asn]['neighbors'][n]['relationship'] == 5:
            # sibling from https://github.com/InetIntel/Improving-Inference-of-Sibling-ASes
            if link[asn]['neighbors'][n]['Active']:
                sibling.append([n,visible_days])
            else:
                sibling_i.append([n,visible_days])
            connections.append((n, asn))
        elif link[asn]['neighbors'][n]['relationship'] == 8: # peer from CAIDA
            if link[asn]['neighbors'][n]['Active']:
                ixp.append([n,visible_days])
            else:
                ixp_i.append([n,visible_days])
            connections.append((n, asn))
        else:
            if link[asn]['category'] == 'tier_1':
                if link[asn]['neighbors'][n]['Active']:
                    customers.append([n, visible_days])
                else:
                    customers_i.append([n, visible_days])
            elif link[n]['category'] == 'tier_1':
                if link[asn]['neighbors'][n]['Active']:
                    providers.append([n,visible_days])
                else:
                    providers_i.append([n,visible_days])
            elif link[asn]['neighbors'][n]['left']:
                if link[asn]['neighbors'][n]['Active']:
                    providers.append([n,visible_days])
                else:
                    providers_i.append([n,visible_days])
                connections.append((n, asn))
            elif link[asn]['neighbors'][n]['right']:
                if link[asn]['neighbors'][n]['Active']:
                    customers.append([n,visible_days])
                else:
                    customers_i.append([n,visible_days])
                connections.append((n, asn))
            else:
                if int(link[asn]['neighbors'][n]['days_left']) > int(link[asn]['neighbors'][n]['days_right']):
                    providers_i.append([n,visible_days])
                else:
                    customers_i.append([n,visible_days])
                connections.append((n, asn))

    t_providers = len(providers)+len(providers_i)
    t_customers = len(customers)+len(customers_i)
    t_peers = len(peers)+len(peers_i)
    max_col = 17
    pos, alpha, line = node_position(customers_i, line= 0, max_col = max_col)
    line += 1
    tmp, tmp2, line = node_position(customers, line= line, max_col = max_col)
    pos.update(tmp)
    alpha.update(tmp2)
    line += 1
    tmp, tmp2, line = node_position(sibling_i, line=line, max_col=max_col)
    pos.update(tmp)
    alpha.update(tmp2)
    line += 1
    tmp, tmp2, line = node_position(sibling, line=line, max_col=max_col)
    pos.update(tmp)
    alpha.update(tmp2)
    line += 1
    pos[asn] = ((max_col/2),line)
    line += 1
    tmp, tmp2, line = node_position(peers, line= line, max_col=max_col)
    pos.update(tmp)
    alpha.update(tmp2)
    line += 1
    tmp, tmp2, line = node_position(peers_i, line= line, max_col=max_col)
    pos.update(tmp)
    alpha.update(tmp2)
    line += 1
    tmp, tmp2, line = node_position(providers, line= line, max_col=max_col)
    pos.update(tmp)
    alpha.update(tmp2)
    line += 1
    tmp, tmp2, line = node_position(providers_i, line= line, max_col=max_col)
    pos.update(tmp)
    alpha.update(tmp2)
    line += 1
    tmp, tmp2, line = node_position(ixp, line=line, max_col=max_col)
    pos.update(tmp)
    alpha.update(tmp2)
    line += 1
    tmp, tmp2, line = node_position(ixp_i, line=line, max_col=max_col)
    pos.update(tmp)
    alpha.update(tmp2)

    node_size = 450
    options = {
        "font_size":5,
        "node_size": node_size,
        "node_color": "white",
        "edgecolors": "black",
        "linewidths": 2,
        "width": 1,
        "alpha": 1,
    }
    graph = nx.DiGraph(name=(str(asn)+' connections'))
    graph.add_edges_from(connections,style=' ', alpha=0.5)

    nx.draw_networkx(graph,pos,arrows=False, **options)
    #Colors -> https://en.wikipedia.org/wiki/X11_color_names
    providers_color = 'darkcyan'
    customers_color = 'darkolivegreen'
    peers_color = 'darkorange'
    ixp_color = 'darkmagenta'
    sibling_color = 'darkviolet'
    providers_i_color = 'cyan'
    customers_i_color = 'olive'
    peers_i_color = 'orange'
    sibling_i_color = 'violet'
    ixp_i_color = 'magenta'

    for n, a in customers_i:
        nx.draw_networkx_nodes(graph, pos, nodelist=[n], alpha=a, node_color=customers_i_color, node_size=node_size)
    for n, a in customers:
        nx.draw_networkx_nodes(graph, pos, nodelist=[n], alpha=a, node_color=customers_color, node_size=node_size)
    for n, a in peers:
        nx.draw_networkx_nodes(graph, pos, nodelist=[n], alpha=a, node_color=peers_color, node_size=node_size)
    for n, a in peers_i:
        nx.draw_networkx_nodes(graph, pos, nodelist=[n], alpha=a, node_color=peers_i_color, node_size=node_size)
    for n, a in providers:
        nx.draw_networkx_nodes(graph, pos, nodelist=[n], alpha=a, node_color=providers_color, node_size=node_size)
    for n, a in providers_i:
        nx.draw_networkx_nodes(graph, pos, nodelist=[n], alpha=a, node_color=providers_i_color, node_size=node_size)
    for n, a in sibling:
        nx.draw_networkx_nodes(graph, pos, nodelist=[n], alpha=a, node_color=sibling_color, node_size=node_size)
    for n, a in sibling_i:
        nx.draw_networkx_nodes(graph, pos, nodelist=[n], alpha=a, node_color=sibling_i_color, node_size=node_size)
    for n, a in ixp:
        nx.draw_networkx_nodes(graph, pos, nodelist=[n], alpha=a, node_color=ixp_color, node_size=node_size)
    for n, a in ixp_i:
        nx.draw_networkx_nodes(graph, pos, nodelist=[n], alpha=a, node_color=ixp_i_color, node_size=node_size)

    img_file = img_folder+str(asn)+'_graph.pdf'
    cut = 1.20

    plt.tight_layout()
    legend = []
    if len(providers_i)>0:
        legend.append(mpatches.Patch(color=providers_i_color, label='Inactive Providers'))
    if len(providers)>0:
        legend.append(mpatches.Patch(color=providers_color, label='Providers'))
    if len(peers_i)>0:
        legend.append(mpatches.Patch(color=peers_i_color, label='Inactive Peers'))
    if len(peers)>0:
        legend.append(mpatches.Patch(color=peers_color, label='Peers'))
    if len(ixp_i)>0:
        legend.append(mpatches.Patch(color=ixp_i_color, label='Inactive IXP'))
    if len(ixp)>0:
        legend.append(mpatches.Patch(color=ixp_color, label='IXP'))
    if len(sibling)>0:
        legend.append(mpatches.Patch(color=sibling_color, label='Siblings'))
    if len(sibling_i) > 0:
        legend.append(mpatches.Patch(color=sibling_i_color, label='Inactive Siblings'))
    if len(customers)>0:
        legend.append(mpatches.Patch(color=customers_color, label='Customers'))
    if len(customers_i)>0:
        legend.append(mpatches.Patch(color=customers_i_color, label='Inactive Customers'))

    title = ('AS' + str(asn) + ' connections \n(' + str(t_providers) + '-Providers, ' + str(t_peers) + '-Peers, ' +
              str(t_customers) + '-Customers)')
    plot_graph(graph, asn, title, img_file, legend)
    table = pd.DataFrame.from_dict(link[asn]['neighbors'])
    title = str(asn) + '(' + link[asn]['category'] + ')'
    as_conections_to_pdf(table, asn, title)


def plot_graph(graph, asn, title, img_file, legend):
    print("Plotting graph from AS", asn)
    nx.drawing.nx_agraph.write_dot(graph, str(asn) + 'teste.dot')
    #plt.legend(handles=legend, fontsize=4, loc='lower right')
    plt.legend(handles=legend, fontsize=4, loc='best', bbox_to_anchor=(0.5, 0., 0.5, 0.5))
    ax = plt.gca()
    ### 2023-11-27
    #plt.figure(num=None, figsize=(20,20), dpi=80)
    #plt.figure(num=None, figsize=(60,40), dpi=80)
    #fig = plt.figure(1)
    #pos = nx.spring_layout(graph)
    #cut = 1.00
    #xmax = cut * max(xx for xx, yy in pos.values())
    #ymax = cut * max(yy for xx, yy in pos.values())
    #plt.xlim(0, xmax)
    #plt.ylim(0, ymax)
    ### end
    ax.margins(0.2)
    #plt.autoscale(False, axis='both', tight=False)
    plt.autoscale(True, axis='both', tight=True)
    plt.title(title)
    plt.axis("off")
    plt.savefig(img_file, format='PDF', bbox_inches="tight")
    plt.clf()
    plt.close()
    plt.cla()
    ### 2023-11-27
    #del fig
    ## end


#https://levelup.gitconnected.com/how-to-write-a-pandas-dataframe-as-a-pdf-5cdf7d525488
def _draw_as_table(df, pagesize, title, footnote=''):
    alternating_colors = [['white'] * len(df.columns), ['lightgray'] * len(df.columns)] * len(df)
    alternating_colors = alternating_colors[:len(df)]
    fig, ax = plt.subplots(figsize=pagesize)
    ax.axis('tight')
    ax.axis('off')
    plt.title(title)
    ax.annotate(footnote,
                xy=(1.0, -0.2),
                xycoords='axes fraction',
                ha='right',
                va="center",
                fontsize=8)
    the_table = ax.table(cellText=df.values,
                        rowLabels=df.index,
                        colLabels=df.columns,
                        rowColours=['lightblue']*len(df),
                        colColours=['lightblue']*len(df.columns),
                        cellColours=alternating_colors,
                        loc='center')
    return fig


def dataframe_to_pdf(df, filename, numpages=(1, 1), pagesize=(11, 8.5), title='', footnote=''):
    with PdfPages(filename) as pdf:
        nh, nv = numpages
        rows_per_page = len(df) // nh
        cols_per_page = len(df.columns) // nv
        for i in range(0, nh):
            for j in range(0, nv):
                page = df.iloc[(i * rows_per_page):min((i + 1) * rows_per_page, len(df)),
                       (j * cols_per_page):min((j + 1) * cols_per_page, len(df.columns))]
                fig = _draw_as_table(page, pagesize, title, footnote)
                if nh > 1 or nv > 1:
                    # Add a part/page number at bottom-center of page
                    #fig.text(0.5, 0.5 / pagesize[0],
                    #         "Part-{}x{}: Page-{}".format(i + 1, j + 1, i * nv + j + 1),
                    #         ha='center', fontsize=8)
                    fig.text(0.5, 0.5 / pagesize[0],
                             "Page-{}".format(i * nv + j + 1),
                             ha='center', fontsize=8)
                pdf.savefig(fig, bbox_inches='tight')

                plt.close()


def as_conections_to_pdf(table, asn, title=''):
    print("Plotting table connections from AS", asn)
    table_file = img_folder + str(asn) + '_table.pdf'
    cols_page = 15
    n_cols = len(table.columns)//cols_page+1
    footnote = ("Relationship = 0:peer,-2:provable provider,-1:provider,1:customer,\n"
                "               2:provable customer,5:sibling,8:IXP connection,9:Unknown")
    dataframe_to_pdf(table, table_file, numpages=(1, n_cols), pagesize=(11, 3), title=title, footnote=footnote)


def as_prefixes_to_pdf(table, asn, title=''):
    table_file = img_folder + str(asn) + '_prefixes_table.pdf'
    cols_page = 9
    n_cols = len(table.columns)//cols_page+1
    footnote = "MOAS = {ASN:days}"
    dataframe_to_pdf(table, table_file, numpages=(1, n_cols), pagesize=(11, 3), title=title, footnote=footnote)


def create_prefix_plot(asn=int, as_info={}):
    print("Plotting prefixes information from AS", asn)
    if not asn in as_info.keys():
        print("ASN", asn," not found on prefixes information")
        return None
    table = pd.DataFrame.from_dict(as_info[asn])
    if '0.0.0.0/0' in table.columns.values.tolist():
        table.drop(['0.0.0.0/0'], inplace=True, axis=1)
    table.drop(['descr'], inplace=True, axis=1)
    title = str(asn) + '(' + as_info[asn]['descr'] + ')'
    as_prefixes_to_pdf(table, asn, title)


def create_graph(asn='', link={}):
    if type(link) is str:
        file = link
        if os.path.isfile():
            try:
                f = open(file,'rb')
                link = pickle.load(f)
                f.close()
            except:
                print('Fail to import information from',file)
                exit(1)
    if not asn in link.keys():
        print("ASN", asn," not found on links information")
        return None
    if not os.path.isdir(img_folder):
        os.mkdir(img_folder)
    neighbors = link[asn]['neighbors']
    connections = []
    providers = []
    customers = []
    peers = []
    providers_i = []
    customers_i = []
    peers_i = []
    for i, n in enumerate(neighbors):
        if link[asn]['neighbors'][n]['relationship'] == 'provider':
            if link[asn]['neighbors'][n]['Active']:
                providers.append(n)
            else:
                providers_i.append(n)
            connections.append((n, asn))
        elif link[asn]['neighbors'][n]['relationship'] == 'customer':
            if link[asn]['neighbors'][n]['Active']:
                customers.append(n)
            else:
                customers_i.append(n)
            connections.append((n, asn))
        elif link[asn]['neighbors'][n]['relationship'] == 'peer':
            if link[asn]['neighbors'][n]['Active']:
                peers.append(n)
            else:
                peers_i.append(n)
            connections.append((n, asn))
        else:
            if link[asn]['neighbors'][n]['left']:
                if link[asn]['neighbors'][n]['Active']:
                    providers.append(n)
                else:
                    providers_i.append(n)
                connections.append((n, asn))
            elif link[asn]['neighbors'][n]['right']:
                if link[asn]['neighbors'][n]['Active']:
                    customers.append(n)
                else:
                    customers_i.append(n)
                connections.append((n, asn))
            else:
                if int(link[asn]['neighbors'][n]['days_left']) > int(link[asn]['neighbors'][n]['days_right']):
                    providers_i.append(n)
                else:
                    customers_i.append(n)
                connections.append((n, asn))

    p_center= len(providers)
    if len(customers)>p_center:
        p_center = len(customers)
    elif len(peers) > p_center:
        p_center = len(peers)
    # set the position according to column (x-coord)
    pos = {n: (i*(p_center/len(providers)+1), 4) for i, n in enumerate(providers+providers_i)}
    pos.update({n: (i*(p_center/len(peers)),3) for i, n in enumerate(peers+peers_i)})
    pos.update({asn: (p_center/2, 2)})
    pos.update({n: (i*(p_center/len(customers))+1, 1) for i, n in enumerate(customers+customers_i)})

    options = {
        #"font_size":7,
        #"node_size": 700,
        #"node_color": "white",
        #"edgecolors": "black",
        #"linewidths": 2,
        #"width": 1,
        #"alpha": 1,
        "pos": (100,100),
    }
    graph = nx.DiGraph(name=(asn+' connections'))
    #graph.add_edges_from(connections,style=' ', alpha=0.5)
    A = nx.nx_agraph.to_agraph(graph)
    A.add_node(asn, **options)
    options = {
        # "font_size":7,
        # "node_size": 700,
        # "node_color": "white",
        # "edgecolors": "black",
        # "linewidths": 2,
        # "width": 1,
        # "alpha": 1,
        "pos": (200, 100),
    }
    A.add_node('1', **options)
    options = {
        # "font_size":7,
        # "node_size": 700,
        # "node_color": "white",
        # "edgecolors": "black",
        # "linewidths": 2,
        # "width": 1,
        # "alpha": 1,
        "pos": (100, 200),
    }
    A.add_node('2', **options)
    options = {
        # "font_size":7,
        # "node_size": 700,
        # "node_color": "white",
        # "edgecolors": "black",
        # "linewidths": 2,
        # "width": 1,
        # "alpha": 1,
        "pos": (100, 200),
    }
    A.add_node('3', **options)
    #A.add_node(pos, nodelist=customers_i, alpha=0.5, node_color=color, node_size=700)
    img_file = img_folder + asn + '.pdf'
    A.layout()
    A.draw(img_file, format='pdf')#, prog="circo")



if __name__ == '__main__':
    file_name = initial_date + '_' + days + '_moas_period.pickle'
    if not os.path.isfile(out_folder + file_name):
        print('Error to found essential file:',file_name)
        exit(1)
    out_file= open(out_folder + file_name, 'rb')
    moas = pickle.load(out_file)
    out_file.close()
    calc_infor = []

    file_name = initial_date + '_' + days + '_link_period.pickle'
    if not os.path.isfile(out_folder + file_name):
        print('Error to found essential file:',file_name)
        exit(1)
    f = open((out_folder + file_name), 'rb')
    link = pickle.load(f)
    f.close()

    file_name = initial_date + '_' + days + '_as-info_period.pickle'
    if not os.path.isfile(out_folder + file_name):
        print('Error to found essential file:',file_name)
        exit(1)
    out_file = open(out_folder + file_name, 'rb')
    as_info = pickle.load(out_file)
    out_file.close()

    as_found = link.keys()
    print(len(as_found), "AS were found in the RIB files from this period (" + str(days), "days)")

    if not os.path.isdir(img_folder):
        os.mkdir(img_folder)

    google = 15169
    globo = 28604
    as1 = 52890
    sibling = 1221
    moas = 4808
    big = 174
    #ases=[globo, google, as1, sibling, moas, big]
    #ases=[3257, 2914, 23764]
    ases = [22548, globo]

    with Pool(processes=n_threads) as th_pool:
        args = [(asn, as_info) for asn in ases]
        th_pool.starmap(create_prefix_plot, args)
        args = [(asn, link, int(days)) for asn in ases]
        th_pool.starmap(create_graph_plot, args)


