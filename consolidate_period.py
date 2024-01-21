#!/usr/bin/python3
import os
from datetime import timedelta, datetime, timezone
import pickle
import numpy as np
import progressbar
# pip install progressbar2
from rov import ROV
# pip install rov
# https://github.com/InternetHealthReport/route-origin-validator
from multiprocessing import Pool
import threading
from config import DEFAULT_IRR_URLS
from config import out_folder, n_threads, initial_date, days
# Folder with information calculated per day

local_n_threads = int(n_threads//2)

def next_date(year_b, month_b, day_b, days):
    """
    Increase n days on date
    :param year_b: start year
    :param month_b: start month
    :param day_b: start day
    :param days: how many days to increase on start date
    :return: year, month, day
    """
    begin = datetime(int(year_b), int(month_b), int(day_b), 0, 0, 0, tzinfo=timezone.utc)
    end_time = begin + timedelta(days=days)
    year = end_time.year
    month = end_time.month
    day = end_time.day
    return year, month, day


def compare_lis(list1=[], list2=[]):
    """
    Compare two list and return a list of equals and a list with different
    :param list1: first list
    :param list2: second list
    :return: equals, different
    """
    list1 = list(list1)
    list2= list(list2)
    equals = []
    different = []
    for l in list1:
        if l in list2:
            equals.append(list1)
            list2.remove(l)
        else:
            different.append(l)
    different = different + list2
    return equals, different


def aggregate_one_as_info(consolidate, asn, as_info, rov, file_n, moas):
    if not asn in consolidate.keys():
        consolidate[asn] = {'descr': '', 'prefixes': {}}
    prefixes_con = list(consolidate[asn]['prefixes'].keys())
    prefixes_con.sort()
    if asn in as_info.keys():
        prefixes = list(as_info[asn].keys())
        prefixes.sort()
    else:
        prefixes = []
    all_prefixes = np.unique(prefixes + prefixes_con)
    for prefix in all_prefixes:
        None
        """
        if not prefix in prefixes_con and prefix != '':
            try:
                state = rov.check(prefix, asn)
            except:
                continue
            prefixes_con.append(prefix)
            if prefix in moas.keys():
                asn_moas = moas[prefix]
            else:
                asn_moas = []
            valid_rpki = state['rpki']['status']
            valid_irr = state['irr']['status']
            if consolidate[asn]['descr'] == '':
                if 'descr' in state['irr'].keys():
                    consolidate[asn]['descr'] = state['irr']['descr']
            if 'country' in state['delegated']['prefix'].keys():
                country = state['delegated']['prefix']['country']
            else:
                country = ''
            consolidate[asn]['prefixes'][prefix] = {'valid': as_info[asn][prefix], 'moas': asn_moas,
                                                    'days_in': 1, 'days_out': file_n, 'max_days_in': 1,
                                                    'max_days_out': file_n, 'valid_rpki': valid_rpki,
                                                    'valid_irr': valid_irr, 'country': country}
        elif prefix in prefixes:
            tmp_di = consolidate[asn]['prefixes'][prefix]['days_in']
            tmp_di += 1
            consolidate[asn]['prefixes'][prefix]['days_in'] = tmp_di
            if tmp_di > consolidate[asn]['prefixes'][prefix]['max_days_in']:
                consolidate[asn][prefix]['prefixes']['max_days_in'] = tmp_di
            consolidate[asn]['prefixes'][prefix]['days_out'] = 0
        else:
            tmp_do = consolidate[asn]['prefixes'][prefix]['days_out']
            tmp_do += 1
            consolidate[asn]['prefixes'][prefix]['days_out'] = tmp_do
            if tmp_do > consolidate[asn]['prefixes'][prefix]['max_days_out']:
                consolidate[asn]['prefixes'][prefix]['max_days_out'] = tmp_do
            consolidate[asn]['prefixes'][prefix]['days_in'] = 0
        """

def aggregate_as_info(files=[], moas={}):
    """
    :param files: list of files names (path) (previously files created , files names end with as-info.pickle)
    :return: a dictionary with AS information
    ASN:{<PREFIX>:{valid:<True/False>, moas:{ASN1:x(days),ASN2:y(days),...},days_in:(how many days was visible), days_out: (how many days wasn't visible),
        max_days_in:(max day visible in sequence), max_days_out:(max day wasn't visible in sequence),
        valid_rpki:<True/False>, valid_irr:<True/False>, country:<country>}
    """
    consolidate = {}
    rov = ROV(irr_urls=DEFAULT_IRR_URLS)
    #rov.download_databases(overwrite=False)
    rov.load_databases()
    for i,f in enumerate(files):
        print(datetime.now(), 'Analysing file', f)
        file_date = f.split("_")[0]
        f = open(f, 'rb')
        as_info = pickle.load(f)
        f.close()
        asns = set(as_info.keys())
        #asns_con = list(consolidate.keys())
        #all_asn = np.unique(asns_con+asns)
        """
        with Pool(processes=local_n_threads) as th_pool:
            args = [(consolidate, asn, as_info, rov, i, moas, ) for asn in all_asn]
            th_pool.starmap(aggregate_one_as_info, args, )
        """
        for asn in asns:
            if not asn in consolidate.keys():
                consolidate[asn] = {'descr': '', 'prefixes': {}, 'first_date': file_date, 'last_date': file_date}
            prefixes_con = set(consolidate[asn]['prefixes'].keys())
            #prefixes_con.sort()
            if asn in as_info.keys():
                prefixes = set(as_info[asn].keys())
                #prefixes.sort()
            else:
                prefixes = set()
            all_prefixes = set(prefixes + prefixes_con)
            for prefix in all_prefixes:
                if not prefix in prefixes_con and prefix != '':
                #if i == 0:
                    try:
                        state = rov.check(prefix, asn)
                    except:
                        continue
                    #prefixes_con.append(prefix)
                    if prefix in moas.keys():
                        asn_moas = moas[prefix]
                    else:
                        asn_moas = []
                    valid_rpki = state['rpki']['status']
                    valid_irr = state['irr']['status']
                    if consolidate[asn]['descr'] == '':
                        if 'descr' in state['irr'].keys():
                            consolidate[asn]['descr'] = state['irr']['descr']
                    if 'country' in state['delegated']['prefix'].keys():
                        country = state['delegated']['prefix']['country']
                    else:
                        country = ''
                    consolidate[asn]['prefixes'][prefix] = {'valid': as_info[asn][prefix], 'moas': asn_moas,
                                                            'days_in': 1, 'days_out': i, 'max_days_in': 1,
                                                            'max_days_out': i, 'valid_rpki': valid_rpki,
                                                            'valid_irr': valid_irr, 'country': country,
                                                            'first_date': file_date, 'last_date': file_date}
                elif prefix in prefixes:
                    tmp_di = consolidate[asn]['prefixes'][prefix]['days_in']
                    consolidate[asn]['prefixes'][prefix]['last_date'] = file_date
                    tmp_di += 1
                    consolidate[asn]['prefixes'][prefix]['days_in'] = tmp_di
                    if tmp_di > consolidate[asn]['prefixes'][prefix]['max_days_in']:
                        consolidate[asn][prefix]['prefixes']['max_days_in'] = tmp_di
                    consolidate[asn]['prefixes'][prefix]['days_out'] = 0
                else:
                    tmp_do = consolidate[asn]['prefixes'][prefix]['days_out']
                    tmp_do += 1
                    consolidate[asn]['prefixes'][prefix]['days_out'] = tmp_do
                    if tmp_do > consolidate[asn]['prefixes'][prefix]['max_days_out']:
                        consolidate[asn]['prefixes'][prefix]['max_days_out'] = tmp_do
                    consolidate[asn]['prefixes'][prefix]['days_in'] = 0
    return consolidate


def aggregate_moas(files=[]):
    """
     :param files: list of files names (path) (previously files created , files names end with moas.pickle)
     :return: a dictionary with MOAS information
     ASN:{<PREFIX>:{ASN:<days>, ASN2:<days>,...}, ASN2:{...
     days = how many days MOAS was visible in the period
     """
    consolidate = {}
    bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength, prefix="Aggregating_MOAS")
    b = 0
    for i, f in enumerate(files):
        f = open(f, 'rb')
        moas = pickle.load(f)
        f.close()
        prefixes = moas.keys()
        for prefix in prefixes:
            b += 1
            bar.update(b)
            con_keys = consolidate.keys()
            if not prefix in con_keys:
                consolidate[prefix]={}
            con_asns = consolidate[prefix].keys()
            for asn in moas[prefix]:
                if asn in con_asns:
                    consolidate[prefix][asn] += 1
                else:
                    consolidate[prefix][asn] = 1
    bar.finish()
    return consolidate

def aggregate_prefixes(files=[]):
    """
     :param files: list of files names (path) (previously files created , files names end with moas.pickle)
     :return: a dictionary with MOAS information
     ASN:{<PREFIX>:{ASN:<days>, ASN2:<days>,...}, ASN2:{...
     days = how many days MOAS was visible in the period
     """
    consolidate = {}
    for i, f in enumerate(files):
        print(datetime.now(), 'Analysing file', f)
        f = open(f, 'rb')
        prefixes_f = pickle.load(f)
        f.close()
        prefixes = prefixes_f.keys()
        for prefix in prefixes:
            con_keys = consolidate.keys()
            if not prefix in con_keys:
                consolidate[prefix]={}
            con_asns = consolidate[prefix].keys()
            for asn in prefixes_f[prefix]:
                if asn in con_asns:
                    consolidate[prefix][asn] += 1
                else:
                    consolidate[prefix][asn] = 1
    return consolidate


def aggregate_one_as_links(consolidate, asn, links):
    # AS path -> <left> <ASN> <right>
    right = links[asn]['right']
    left = links[asn]['left']
    con_keys = consolidate.keys()
    if asn in con_keys:
        neighbors_con = consolidate[asn]['neighbors'].keys()
    else:
        consolidate[asn] = {'neighbors': {}}
        neighbors_con = []
    #neighbors = np.unique(right + left)
    neighbors = set(right + left)
    for n in neighbors:
        if not n in neighbors_con:
            consolidate[asn]['neighbors'][n] = {'Active': True, 'right': False, 'days_right': 0, 'left': False,
                                                'days_left': 0, 'relationship': 9}
        consolidate[asn]['neighbors'][n]['Active'] = True
        if n in right:
            consolidate[asn]['neighbors'][n]['right'] = True
            consolidate[asn]['neighbors'][n]['days_right'] += 1
            n_pos = right.index(n)
            consolidate[asn]['neighbors'][n]['relationship'] = links[asn]['right_rel'][n_pos]
        else:
            consolidate[asn]['neighbors'][n]['right'] = False
        if n in left:
            consolidate[asn]['neighbors'][n]['left'] = True
            consolidate[asn]['neighbors'][n]['days_left'] += 1
            n_pos = left.index(n)
            consolidate[asn]['neighbors'][n]['relationship'] = links[asn]['left_rel'][n_pos]
        else:
            consolidate[asn]['neighbors'][n]['left'] = False

    neighbors_con = consolidate[asn]['neighbors'].keys()
    equals, different = compare_lis(neighbors, neighbors_con)
    for n in different:
        consolidate[asn]['neighbors'][n]['Active'] = False
        consolidate[asn]['neighbors'][n]['left'] = False
        consolidate[asn]['neighbors'][n]['right'] = False


def aggregate_as_links(files_links=[], file_as_rel=''):
    """
    Agratate all AS link information form files and set AS relationship with neighbors
    :param files_links: AS link files previously calculated
    :param file_as_rel: AS relationship previously calculated
    :return: A consolidate dictionary.
    Dictionary format:
    {AS:{neighbors:{ASN:{Active:<True/False>, right:<True/False>, days_right: 0, left:<True/False>, days_left: 0,
    relationship:<0=peer/-1=provider/-2=probable provider/1=customer/2=probable customer/5=sibling/9=unknown>,}...}
    ,category:<tier_1/tier_2/ixp/other>}, ASN:{....
    """
    consolidate = {}
    f = open(file_as_rel, 'rb')
    as_rel = pickle.load(f)
    f.close()
    tier_1 = as_rel['tier-1']
    tier_1 = [int(x) for x in tier_1]
    ixp = as_rel['ixp']
    ixp = [int(x) for x in ixp]
    bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength, prefix="Aggregating_AS_links")
    b = 0
    for day, f in enumerate(files_links):
        print(datetime.now(), 'Analysing file',f)
        f = open(f, 'rb')
        links = pickle.load(f)
        asns = links.keys()
        """
        with Pool(processes=local_n_threads) as th_pool:
            args = [(consolidate, asn, links) for asn in asns]
            th_pool.starmap(aggregate_one_as_links, args, )
        """
        for asn in asns:
            # AS path -> <left> <ASN> <right>
            right = links[asn]['right']
            left = links[asn]['left']
            con_keys = consolidate.keys()
            if asn in con_keys:
                neighbors_con = consolidate[asn]['neighbors'].keys()
            else:
                consolidate[asn] = {'neighbors': {}}
                neighbors_con = []
            #Fazer o teste com set() ao invés de List()
            #neighbors = np.unique(right + left)
            neighbors = set(right + left)
            for n in neighbors:
                if not n in neighbors_con:
                    consolidate[asn]['neighbors'][n] = {'Active': True, 'right': False, 'days_right': 0, 'left': False,
                                                        'days_left': 0, 'relationship': 9}
                consolidate[asn]['neighbors'][n]['Active'] = True
                if n in right:
                    consolidate[asn]['neighbors'][n]['right'] = True
                    consolidate[asn]['neighbors'][n]['days_right'] += 1
                    n_pos = right.index(n)
                    consolidate[asn]['neighbors'][n]['relationship'] = links[asn]['right_rel'][n_pos]
                else:
                    consolidate[asn]['neighbors'][n]['right'] = False
                if n in left:
                    consolidate[asn]['neighbors'][n]['left'] = True
                    consolidate[asn]['neighbors'][n]['days_left'] += 1
                    n_pos = left.index(n)
                    consolidate[asn]['neighbors'][n]['relationship'] = links[asn]['left_rel'][n_pos]
                else:
                    consolidate[asn]['neighbors'][n]['left'] = False

            neighbors_con = consolidate[asn]['neighbors'].keys()
            equals, different = compare_lis(neighbors, neighbors_con)
            for n in different:
                consolidate[asn]['neighbors'][n]['Active'] = False
                consolidate[asn]['neighbors'][n]['left'] = False
                consolidate[asn]['neighbors'][n]['right'] = False
        f.close()
    asns = consolidate.keys()
    for asn in asns:
        b+=1
        bar.update(b)
        neighbors = consolidate[asn]['neighbors'].keys()
        if asn in tier_1:
            as_cat = 'tier_1'
        elif asn in ixp:
            as_cat = 'ixp'
        else:
            if len(neighbors) == 1:
                # AS has only one connection to access the internet
                as_cat = 'Stub'
            else:
                as_cat = 'other'
                has_customer = False
                as_cat_tmp = ''
                for n in neighbors:
                    if consolidate[asn]['neighbors'][n]['relationship'] in [1,2]:
                        has_customer = True
                    if n in tier_1 and as_cat == 'other':
                        as_cat_tmp = 'tier_2'
                    if as_cat_tmp == 'tier_2' and has_customer:
                        as_cat = as_cat_tmp
                        break

        consolidate[asn]['category'] = as_cat
    bar.finish()
    return consolidate


def select_files(initial_date='20230101',days=300):
    files = os.listdir(out_folder)
    selected_files = {'as-info':[],'as-rel':[], 'link':[], 'moas':[], 'prefixes':[]}
    file_asinfor = 'as-info.pickle'
    file_asrel = 'as-rel.pickle'
    file_link = 'link.pickle'
    file_moas = '_moas.pickle'
    file_prefixes = '_prefix.pickle'
    year_b = initial_date[0:4]
    month_b = initial_date[4:6]
    day_b = initial_date[6:8]
    for d in range(days):
        year, month, day = next_date(year_b, month_b, day_b, d)
        month = str(month)
        day = str(day)
        if len(month) < 2:
            month = '0' + month
        if len(day) < 2:
            day = '0' + day
        date = str(year) + month + day
        for f in files:
            if f.startswith(date):
                if f.endswith(file_asinfor):
                    selected_files['as-info'].append(out_folder+f)
                elif f.endswith(file_asrel ):
                    selected_files['as-rel'].append(out_folder+f)
                elif f.endswith(file_link):
                    selected_files['link'].append(out_folder+f)
                elif f.endswith(file_moas):
                    selected_files['moas'].append(out_folder+f)
                elif f.endswith(file_prefixes):
                    selected_files['prefixes'].append(out_folder+f)

    return selected_files


def th_aggregate_as_links(initial_date, days, files=[]):
    file_name = initial_date + '_' + days + '_link_period.pickle'
    if not os.path.isfile(out_folder + file_name):
        link = aggregate_as_links(files_links=files['link'], file_as_rel=files['as-rel'][-1])
        out_file = open(out_folder + file_name, 'wb')
        pickle.dump(link, out_file)
        out_file.close()
    else:
        print("Previous file was computed:", file_name)


def th_aggregate_as_info(initial_date, days, files=[], moas={}):
    file_name = initial_date + '_' + days + '_as-info_period.pickle'
    if not os.path.isfile(out_folder + file_name):
        as_info = aggregate_as_info(files=files['as-info'], moas=moas)
        out_file = open(out_folder + file_name, 'wb')
        pickle.dump(as_info, out_file)
        out_file.close()
    else:
        print("Previous file was computed:", file_name)


def th_aggregate_prefixes(initial_date, days, files=[]):
    file_name = initial_date + '_' + days + '_prefixes_period.pickle'
    if not os.path.isfile(out_folder + file_name):
        prefixes = aggregate_prefixes(files=files['prefixes'])
        out_file = open(out_folder + file_name, 'wb')
        pickle.dump(prefixes, out_file)
        out_file.close()
    else:
        print("Previous file was computed:", file_name)


def pool_control(args):
    exec_type = args[0]
    if exec_type == 1:
        th_aggregate_as_links(args[1], args[2], args[3])
    elif exec_type == 2:
        th_aggregate_as_info(args[1], args[2], args[3], args[4])
    elif exec_type == 3:
        th_aggregate_prefixes(args[1], args[2], args[3])

if __name__ == '__main__':
    files = select_files(initial_date=initial_date, days=int(days))
    file_name = initial_date + '_' + days + '_moas_period.pickle'
    if not os.path.isfile(out_folder + file_name):
        moas = aggregate_moas(files=files['moas'])
        out_file = open(out_folder + file_name, 'wb')
        pickle.dump(moas, out_file)
        out_file.close()
    else:
        print("Previous file was computed:", file_name)
    out_file = open(out_folder + file_name, 'rb')
    moas = pickle.load(out_file)
    out_file.close()
    calc_infor = []

    with Pool(processes=n_threads) as th_pool:
        args=[]
        args.append([(1, initial_date, days, files)]) #th_aggregate_as_links
        args.append([(2, initial_date, days, files, moas)]) #th_aggregate_as_info
        args.append([(3, initial_date, days, files)]) #th_aggregate_prefixes
        th_pool.starmap(pool_control, args,)
    """
    th1 = threading.Thread(target=th_aggregate_as_links, args=(initial_date, days, files,))
    th1.start()
    th2 = threading.Thread(target=th_aggregate_as_info, args=(initial_date, days, files, moas,))
    th2.start()
    with Pool(processes=local_n_threads) as th_pool:
        args = [(initial_date, days, files)]
        th_pool.starmap(th_aggregate_prefixes, args)
    #th3 = threading.Thread(target=th_aggregate_prefixes, args=(initial_date, days, files))
    #th3.start()
    th1.join()
    th2.join()
    #th3.join()
    """