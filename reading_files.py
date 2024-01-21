#!/usr/bin/python3
from multiprocessing import Pool
import logging
from check_network_objects import is_valid_prefix, is_valid_asn
import json
import os
import bz2
import pickle
from datetime import timedelta, datetime, timezone
from config import out_folder, tmp_dir, n_threads

# Json with sibling information
# from https://github.com/InetIntel/Improving-Inference-of-Sibling-ASes
sibling_file = 'Sibling/ii.as-org.v01.2023-01.json'
# Set bgpscanner if it isn't in user PATH
bgpscanner = 'bgpscanner'
# Erase temp files when finish
clean_tmp_files = True
# Log file with statistics and other information
# If remove it, comment the last line
log_file = str(datetime.now()).replace(":", "").replace(" ", "_")+'.log'
logging.basicConfig(filename=log_file, filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)


def valley_free_verification(as_path_rel=[]):
    """
    :param as_path_rel: list of ASN, AS path relationship information
    :return: Valley Free is True or False
    Sequence in AS path -1 P2C , 1 C2P  or
    -1, * , 1 = Valley-free violation
    """
    got_provider = False
    for apr in as_path_rel:
        if apr == -1:
            got_provider = True
        elif got_provider:
            if apr == 1:
                return False
    # with regex
    # valley_expr = "*[-2,0-9]+[-1]*[-2,-1,0,2-9]+[1]*[-2,-1,0,-9]"
    # evaluate = ''.join(str(x) for x in as_path_rel)
    # if re.search(valley_expr, evaluate):
    #    return False

    return True


def mtr_to_txt(file=''):
    """
    Convert file from Route Views or RIPE to TXT file (or bz2)
    :param file: path of file from Route Views or RIPE RIS
    :return: TXT file name (or bz2)
    Output bgpscanner format:
    line.split('|")
      0     1        2      3        4          5             6           7         8       9          10
    TYPE|SUBNETS|AS_PATH|NEXT_HOP|ORIGIN|ATOMIC_AGGREGATE|AGGREGATOR|COMMUNITIES|SOURCE|TIMESTAMP|ASN 32 BIT
    Output file format:
      0        1
    SUBNETS|AS_PATH
    """

    out_file = file.split('/')
    if len(out_file) > 1:
        out_file = out_file[-2] + '_' + out_file[-1]
    else:
        out_file = out_file[-1]
    out_file = out_file.replace('bz2', 'txt')
    # Filter to get only information the code need (subnet and AS_path)
    filter = " | awk -F '|' '{ print $2\"|\"$3}' "
    out_file = tmp_dir + '/' + out_file
    end_cmd = ' 1> ' + out_file + '.tmp'
    error_redirect = " 2> /dev/null "
    cmd = bgpscanner + ' -L ' + file + filter + end_cmd + error_redirect
    if os.path.isfile(out_file):
        print('Loading information precomputed from file:', out_file)
    else:
        try:
            print('Reading file:', file)
            os.system(cmd)
            os.rename((out_file + '.tmp'), out_file)
        except:
            logging.info("Fail to read file " + file)
            print('Fail to read', file)
            return None

    return out_file


def next_date(year_b, month_b, day_b, days):
    """
    Increase n days on date
    :param year_b: starting year
    :param month_b: starting month
    :param day_b: starting day
    :param days: how many days to increase on starting date
    :return: year, month, day
    """
    begin = datetime(int(year_b), int(month_b), int(day_b), 0, 0, 0, tzinfo=timezone.utc)
    end_time = begin + timedelta(days=days)
    year = end_time.year
    month = end_time.month
    day = end_time.day
    return year, month, day


def read_caida_as_rel(file=''):
    """
    Read CAIDA relationships and create a dictionary with information
    :param file: CAIDA relationship file name (path)
    :return: dictionary with the information
    dictionary format:
    {asn : {asn :(-1=provider, 0=peer, 1=customer).. 'tier-1' : [ASN Tier1], 'ixp' : [ASN IXP] }
    The key tier-1 has Tier 1 ASNs
    The key ixp has IXP ASNs
    Other keys are ASNs number and their connections relationships
    Label from CAIDA files
    # 0 = p2p
    # -1 = p2c
    """
    lines = bz2.open(file, 'rt')
    tier1 = []
    ixp = []
    as_rel = {}
    for line in lines:
        line = line.strip()
        if line.startswith('#'):
            if line.startswith("# input"):
                tmp = line.split(':')
                tmp = tmp[-1]
                tmp = tmp.strip()
                tier1 = tmp.split(' ')
                continue
            elif line.startswith('# IXP'):
                tmp = line.split(':')
                tmp = tmp[-1]
                tmp = tmp.strip()
                ixp = tmp.split(' ')
                continue
            else:
                continue
        tmp = line.split('|')
        if len(tmp) < 3:
            continue
        asn1 = int(tmp[0])
        asn2 = int(tmp[1])
        rel = int(tmp[2])
        keys = as_rel.keys()
        if not (asn1 in keys):
            as_rel[asn1] = {}
        if not (asn2 in keys):
            as_rel[asn2] = {}
        if rel == 0:
            if not (asn2 in as_rel[asn1].keys()):
                as_rel[asn1][asn2] = 0
            if not (asn1 in as_rel[asn2].keys()):
                as_rel[asn2][asn1] = 0
        elif rel == (-1):
            if not (asn2 in as_rel[asn1].keys()):
                as_rel[asn1][asn2] = 1  # asn1=provider asn2=customer
            if not (asn1 in as_rel[asn2].keys()):
                as_rel[asn2][asn1] = -1  # asn2=customer asn1=provider
    as_rel['tier-1'] = tier1
    as_rel['ixp'] = ixp
    return as_rel


def get_files(folder='./rib'):
    """
    :param folder: Folder with sub folders with rib files
    :return: two lists with files, one with gz files (ris) and other with bz2 files (rv)
    """
    files_rv = []
    files_ris = []
    folders = []
    folders.append(folder)
    while len(folders) > 0:
        f = folders.pop()
        with os.scandir(f) as it:
            for entry in it:
                if entry.is_dir():
                    folders.append(entry.path)
                elif not entry.name.startswith('.') and entry.is_file():
                    if entry.name.endswith('.gz'):
                        files_ris.append(entry.path)
                    elif entry.name.endswith('.bz2'):
                        files_rv.append(entry.path)
    return files_rv, files_ris


def date_from_file_name(file=''):
    """
    :param file: path with standard file name Route Views or RIPE RIS (str)
    :return: yyyymmdd from file name (str)
    file name format example rib.20230105.0000.bz2 or bview.20230105.0000.gz
    return 20230105
    """
    name = file.split('/')
    name = name[-1]
    name = name.split('.')
    day = name[1]
    return day


def only_as_path(file=''):
    """
    Extract only AS path from TXT files and remove duplicates lines
    :param file: TXT file with information form Route Views or RIPE rib files
    :return: file name with all AS path, one per line
    * Only run on Linux, because needs shell commands: bzcat or cat, awk, sort and uniq
    """
    filter = "awk -F '|' '{ print $2}' "
    tmp = file.split('/')
    tmp = tmp[-1]
    tmp = tmp.split('.')
    tmp = tmp[:-1]
    tmp2 = ''
    for t in tmp:
        tmp2 = tmp2 + t + '_'

    out_file = tmp_dir + 'as_path_' + tmp2 + '.txt'
    cmd = 'cat ' + file + ' | ' + filter
    if not os.path.isfile(out_file):
        if os.path.isfile(file):
            cmd = cmd + ' | sort | uniq  > ' + out_file + '.tmp'
        os.system(cmd)
        os.rename((out_file+'.tmp'), out_file)
    return out_file

def only_prefix_asn(file=''):
    """
    Extract only prefixes and origin ASN from TXT files and remove duplicates lines
    :param file: TXT file with information form Route Views or RIPE rib files
    :return: file name with prefix and origin ASN
    file format:
    <prefix>|<ASN>
    e.g.
    100.112.0.0/16|30793
    * Only run on Linux, because needs shell commands: awk, sort and uniq
    """
    filter = "awk -F '|' '{ print $1\" \"$2 }'| awk '{print $1\"|\"$NF}'"
    tmp = file.split('/')
    tmp = tmp[-1]
    tmp = tmp.split('.')
    tmp = tmp[:-1]
    tmp2 = ''
    for t in tmp:
        tmp2 = tmp2 + t + '_'
    out_file = tmp_dir + '/prefix_' + tmp2 + '.txt'
    cmd = 'cat ' + file + ' | ' + filter
    if not os.path.isfile(out_file):
        if os.path.isfile(file):
            cmd = cmd + " | sort | uniq  > " + out_file + '.tmp'
        os.system(cmd)
        os.rename((out_file + '.tmp'), out_file)
    return out_file


def concat_info_from_files(files=[], files_date=''):
    """
    Create two files with information from a list of TXT files with RIB information
    :param files: a list with n files
    :param files_date: date files (yyyymmdd)
    :return: out_file_path, out_file_prefix
    out_file_path = file name create with all AS path form files
    out_file_prefix = file name create with all prefixes and originate ASNs
    """
    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)
    as_path_files = []
    prefix_files = []
    print('Removing duplicate information from any file')
    out_file = tmp_dir + files_date + 'tmp.txt'
    out_file_path = tmp_dir + files_date + '_path.txt'
    out_file_prefix = tmp_dir + files_date + '_prefix.txt'

    if not os.path.isfile(out_file_path):
        for i, file in enumerate(files):
            f_path = only_as_path(file)
            as_path_files.append(f_path)
        print('Removing duplicate information between files')
        for i, file in enumerate(as_path_files):
            cmd = 'cat ' + file + ' >> ' + out_file
            os.system(cmd)
            os.remove(file)
        cmd = "sort " + out_file + " | uniq  > " + out_file_path + '.tmp'
        os.system(cmd)
        os.rename((out_file_path + '.tmp'), out_file_path)
        os.remove(out_file)
    else:
        print('Loading information precomputed from file ', out_file_path)
    out_file2 = tmp_dir + '/' + files_date + 'tmp2.txt'
    if not os.path.isfile(out_file_prefix):
        for i, file in enumerate(files):
            f_prefix = only_prefix_asn(file)
            prefix_files.append(f_prefix)
            os.remove(file)
        print('Removing duplicate information between files')
        for i, file2 in enumerate(prefix_files):
            cmd = 'cat ' + file2 + ' >> ' + out_file2
            os.system(cmd)

        cmd = "sort " + out_file2 + " | uniq  > " + out_file_prefix + '.tmp'
        os.system(cmd)
        os.rename((out_file_prefix + '.tmp'), out_file_prefix)
        os.remove(out_file2)
    else:
        print('Loading information precomputed from file ', out_file_prefix)
    return out_file_path, out_file_prefix


# It gets all asn connections that exist in BGP announce, based on AS path in a day (file)
def get_as_conections(file, as_links={}, as_rel={}):
    """
    read a file with AS path and create a dictionary with ASN and its connections
    :param file: file name with one AS path per line
    :param as_links: dictionary previously created to be incremented or a blank
    :return: a dictionary with ASN and its connections observed in AS path in file
    dictionary format:
    {ASN: {'left':[ASNs], 'left_rel':[<0=peer/-1=provider/1=customer/5=sibling/8=IXP connection/9=unknown>],
    'right':[ASNs], 'right_rel':[<0=peer/-1=provider/1=customer/5=sibling/8=IXP connection/9=unknown>]
    Where: AS path (... <left> <ASN> <right> ...)
    """
    if file.endswith('.bz2'):
        lines = bz2.open(file, 'rt')
    else:
        lines = open(file, 'r')
    if lines is None:
        return as_links
    print('Analysing information from file (AS_path).', file)
    if os.path.isfile(sibling_file):
        sf = open(sibling_file, 'r')
        sibling = json.load(sf)
        sf.close()
    else:
        sibling = {}
    asn_rel = as_rel.keys()
    valley_free_violation = list()
    ixp = as_rel['ixp']
    total_lines = 0

    for j, line in enumerate(lines):
        total_lines += 1
        if not '{' in line:
            line = line.strip()
            as_path = line.split(' ')
            original_as_path = []
            path = []
            p_ans = 0
            # Remove invalid ASN and prepeends
            for asn_pt in as_path:
                try:
                    asn_p = int(asn_pt) # incluido
                except:
                    continue
                if is_valid_asn(asn_p) and asn_p != p_ans:
                    path.append(asn_p)
                original_as_path.append(asn_p)
                p_ans = asn_p
            n_asn = len(path)
            vect_as_path_rel = []
            for i, asn_p in enumerate(path):
                if not asn_p in as_links:
                    as_links[asn_p] = {'left': [], 'left_rel': [], 'right': [], 'right_rel': []}
                link_up = as_links[asn_p]['left']
                link_down = as_links[asn_p]['right']
                if i>0:
                    asn_l = path[i-1]
                    relationship = 9  # unkown
                    if path[i-1] in ixp:
                        relationship = 8 # connection with IXP
                    elif str(asn_p) in sibling.keys():
                        if str(asn_l) in sibling[str(asn_p)]['Sibling ASNs']:
                            relationship = 5 # Sibling
                    if asn_p in asn_rel and relationship == 9:
                        if asn_l in as_rel[asn_p].keys():
                            relationship = as_rel[asn_p][asn_l]
                        elif len(vect_as_path_rel) > 0:
                            if vect_as_path_rel[-1] == 0 or vect_as_path_rel[-1] == -1:
                                relationship= -2 # probable p2c
                            elif vect_as_path_rel[-1] == 1:
                                relationship = 2 # probable c2p

                    vect_as_path_rel.append(relationship)
                    if not(path[i-1] in link_up):
                        as_links[asn_p]['left'].append(int(path[i-1]))
                        as_links[asn_p]['left_rel'].append(relationship)
                if i < (n_asn-1):
                    asn_r = path[i - 1]
                    relationship = 9  # unkown
                    if path[i - 1] in ixp:
                        relationship = 8  # connection with IXP
                    elif str(asn_p) in sibling.keys():
                        if str(asn_r) in sibling[str(asn_p)]['Sibling ASNs']:
                            relationship = 5  # Sibling
                    if asn_p in asn_rel and relationship == 9:
                        if asn_r in as_rel[asn_p].keys():
                            relationship = as_rel[asn_p][asn_r]
                        elif len(vect_as_path_rel) > 0:
                            if vect_as_path_rel[-1] == 0 or vect_as_path_rel[-1] == -1:
                                relationship = -2  # probable p2c
                            elif vect_as_path_rel[-1] == 1:
                                relationship = 2  # probable c2p
                    if not(path[i+1] in link_down):
                        as_links[asn_p]['right'].append(int(path[i+1]))
                        as_links[asn_p]['right_rel'].append(relationship)
        valley_free = valley_free_verification(vect_as_path_rel)
        if valley_free == False:
                tmp = [original_as_path, vect_as_path_rel]
                valley_free_violation.append(tmp)
    lines.close()
    total_violations = len(valley_free_violation)
    p_viol = round((total_violations / total_lines), 5) * 100
    logging.info(file + " Total AS path = " + str(total_lines) + " | Total violations = " + str(total_violations) +
          " " + str(p_viol)+' %')
    return as_links, valley_free_violation


def get_as_info(file, as_info = {}, prefixes_info={}):
    """
    read a file with prefix and originate ASN and create a dictionary with ASN and its prefixes observed in the files
    :param file: TXT file name with prefix|ASN format
    :param as_info: dictionary previously created to be incremented or a blank
    :return: a dictionary with ASN and its prefixes
    dictionary format:
    {"ASN":{<prefix>: <valid prefix (true or false)>, <prefix>:...}, "ASN":{....
    """
    if file.endswith('.bz2'):
        lines = bz2.open(file, 'rt')
    else:
        lines = open(file, 'r')
    if lines is None:
        return None, None
    print('Analysing information from file (Prefix)', file)
    for i, line in enumerate(lines):
        tmp = line.split('|')
        if len(tmp)!= 2:
            logging.info('Error ' + file + ' Line nr ' + str(i+1) + ' : ' + line)
            continue
        prefix = tmp[0]
        tmp = tmp[1].split(' ')
        try:
            prefix_asn = int(tmp[-1])
        except:
            if not '{' in line:
                logging.info('Prefix error ' + file + ' Line nr ' + str(i+1) + ' : ' + line)
            continue
        if not prefix_asn in as_info.keys():
            valid_prefix = is_valid_prefix(prefix)
            as_info[prefix_asn] = {prefix: valid_prefix}
        elif not prefix in as_info[prefix_asn]:
            valid_prefix = is_valid_prefix(prefix)
            as_info[prefix_asn][prefix] = valid_prefix

        if not prefix in prefixes_info.keys():
            prefixes_info[prefix] = [prefix_asn]
        elif not prefix_asn in prefixes_info[prefix]:
            prefixes_info[prefix].append(prefix_asn)
    lines.close()
    return as_info, prefixes_info


def check_moas(prefixes_info={}):
    prefixes = prefixes_info.keys()
    moas={}
    for prefix in prefixes:
        if len(prefixes_info[prefix])>1:
            moas[prefix] = prefixes_info[prefix]
    return moas


def create_info_files(files=[], files_date='', as_rel={}):
    """
    Call other functions and save the dictionaries on file system
    :param files: TXT files with information from IRB files, all files from same date
    :param files_date: date files (yyyymmdd)
    :return: nothing, all information will be saved in the file system

    """
    as_links = {}
    as_info = {}
    if not os.path.isdir(out_folder):
        os.mkdir(out_folder)
    out_file_path, out_file_prefix = concat_info_from_files(files, files_date)

    pkl_file = out_folder + '/' + files_date + '_link.pickle'
    if not os.path.isfile(pkl_file):
        as_links, valley_free_violation = get_as_conections(out_file_path, as_links, as_rel)
        with open(pkl_file, "wb") as outfile:
            pickle.dump(as_links, outfile)
            outfile.close()
        valley_f = out_folder + '/' + files_date + '_vfv.pickle'

        with open(valley_f, "wb") as outfile:
            pickle.dump(valley_free_violation, outfile)
            outfile.close()

    pkl_file = out_folder + '/' + files_date + '_as-info.pickle'
    if not os.path.isfile(pkl_file):
        as_info, prefixes_info = get_as_info(out_file_prefix, as_info)
        with open(pkl_file, "wb") as outfile:
            pickle.dump(as_info, outfile)
            outfile.close()
        pkl_file_prefix = out_folder + '/' + files_date + '_prefix.pickle'
        with open(pkl_file_prefix, "wb") as outfile2:
            pickle.dump(prefixes_info, outfile2)
            outfile2.close()
        pkl_file_moas = out_folder + '/' + files_date + '_moas.pickle'
        moas = check_moas(prefixes_info)
        with open(pkl_file_moas, "wb") as outfile3:
            pickle.dump(moas, outfile3)
            outfile2.close()


def caida_files_as_rel(folder='./caida'):
    """
    Read CAIDA relationship files from out_folder and return a dictionary with files name and dates
    :param folder: out_folder which has CAIDA relationships files (file name format yyyymmdd.as-rel2.txt.bz2)
    :return: a dictionary with dates and files names
    """
    caida_f = os.listdir(folder)
    files_caida = {}
    for file in caida_f:
        if file.endswith("as-rel2.txt.bz2"):
            tmp = file.split('.')
            files_caida[tmp[0]] = folder + '/' + file
    return files_caida


def separate_per_day(files=[]):
    """
    Get a list with files name and separate then by date in the file name
    :param files: a list of files names
    :return: a dictionary with files names and their dates
    file name format rib.20230105.0000.bz2 or bview.20230101.0000.gz
    """
    files_per_day = {}
    for i, file in enumerate(files):
        p_date = date_from_file_name(file)
        keys = files_per_day.keys()
        if p_date in keys:
            files_per_day[p_date].append(file)
        else:
            files_per_day[p_date] = [file]
    return files_per_day


def create_relationship_file(d, caida_files):
    pkl_file = out_folder + '/' + d + '_as-rel.pickle'
    if not os.path.isfile(pkl_file):
        as_rel = read_caida_as_rel(caida_files[d])
        with open(pkl_file, "wb") as outfile:
            pickle.dump(as_rel, outfile)
            outfile.close()



def create_files_with_threads(d, dict_files):
    start_day = datetime.now()
    print('Starting day:', d, 'at', start_day)
    logging.info('Starting day: ' + d)
    caida_file = d[:6] + '01'
    pkl_file = out_folder + '/' + caida_file + '_as-rel.pickle'
    if not os.path.isfile(pkl_file):
        print('CAIDA file from date was no found (' + caida_file + ')')
        exit(1)
    else:
        file_load = open(pkl_file, 'rb')
        as_rel = pickle.load(file_load)
        file_load.close()
    day_files = dict_files[d]
    txt_files = []
    pkl_file_moas = out_folder + '/' + d + '_moas.pickle'
    pkl_file_as_info = out_folder + '/' + d + '_as-info.pickle'
    pkl_file_link = out_folder + '/' + d + '_link.pickle'
    if not (os.path.isfile(pkl_file_moas) and os.path.isfile(pkl_file_as_info) and os.path.isfile(pkl_file_link)):
        out_file_path = tmp_dir + '/' + d + '_path.txt'
        out_file_prefix = tmp_dir + '/' + d + '_prefix.txt'
        if not (os.path.isfile(out_file_path) and os.path.isfile(out_file_prefix)):
            for df in day_files:
                txt_files.append(mtr_to_txt(df))
        create_info_files(txt_files, d, as_rel)
        if clean_tmp_files:
            for txt_file in txt_files:
                if os.path.isfile(txt_file):
                    os.remove(txt_file)
    end_day = datetime.now()
    logging.info('Finishing day: ' + d + ' Total time: ' + str(end_day - start_day))


if __name__ == '__main__':
    start_time = datetime.now()
    print('Starting at', start_time)
    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)
    files_rv, files_ris = get_files('./rib')
    files = files_rv + files_ris
    dict_files = separate_per_day(files)
    files_dates = list(dict_files.keys())
    files_dates.sort()
    caida_files = caida_files_as_rel('./caida')
    caida_data_files = caida_files.keys()
    if not os.path.isdir(out_folder):
        os.mkdir(out_folder)

    with Pool(processes=n_threads) as th_pool:
        args = [(d, caida_files) for d in caida_data_files]
        th_pool.starmap(create_relationship_file, args)

    with Pool(processes=n_threads) as th_pool:
        args = [(d, dict_files) for d in files_dates]
        th_pool.starmap(create_files_with_threads, args)
    """
    if clean_tmp_files:
        files = os.listdir(tmp_dir)
        for file in files:
            if os.path.isfile(tmp_dir + '/' + file):
                os.remove(tmp_dir + '/' + file)
        os.rmdir(tmp_dir)
    """
    end_time = datetime.now()
    logging.info('Total time execution = '+ str(end_time-start_time))
