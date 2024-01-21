#!/usr/bin/python3
import urllib.request, urllib.error, urllib.parse
from bs4 import BeautifulSoup
#pip install beautifulsoup4
from config import (rv_url, rv_collectors, ripe_url, ripe_collectors, rv_type_file, ripe_type_file,
                    caida_type_file, caida_rel_url)
import progressbar
import time
import os
from random import uniform
from datetime import timedelta, datetime, timezone
import argparse


def arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', '--year_b', required=True)
    parser.add_argument('-m', '--month_b', required=True)
    parser.add_argument('-d', '--day_b', required=True)
    parser.add_argument('-D', '--days', required=True)
    parser.add_argument('-r', '--re_download')
    parser.add_argument('-p', '--projects')
    parser.add_argument('-t', '--file_type', required=True)
    parser.add_argument('-a', '--all')
    # parser.add_argument('-v', dest='verbose', action='store_true')
    args = parser.parse_args()
    try:
        year_b = int(args.year_b)
        month_b = int(args.month_b)
        day_b = int(args.day_b)
    except:
        print(
            "Syntaxe -y (year begin) -m (month begin) -d (day begin) -D (how many days) -r (download files again y/n) "
            "-p (projects rv/ripe) -t (type of files, rib or updates) -a (y for all files or n for one file per day/collector)"
        )
        exit(2)
    ftype = args.file_type
    if ftype != 'rib' and ftype != 'updates':
        print("Usage -t rib or -t updates")
        exit(2)
    re_download = args.re_download
    projects = args.projects
    if projects is None:
        projects = ['rv', 'ripe', 'caida']
    elif projects == 'rv' or projects == 'ripe':
        projects = [projects, 'caida']
    else:
        print('Usage -p rv for routeviews, -p ripe for RIPE RIS or do not usage to download both projects '
              'and CAIDA relationship files')

    days = args.days
    if re_download is None or re_download == 'n':
        re_download = False
    elif re_download == 'y':
        re_download = True
    if days == None:
        days = 1
    else:
        days = int(days)
    all = args.all
    if all is None or all == 'n':
        all = False
    elif all == 'y':
        all = True
    if (year_b is None) or (month_b is None) or (day_b is None) or (ftype is None):
        print(
            "Syntaxe -y (year begin) -m (month begin) -d (day begin) -D (how many days) -r (download files again y/n) "
            "-p (projects rv/ripe) -t (type of files, rib or updates) -a (y for all files or n for one file per day/collector)"
        )
        exit(2)
    year_b = str(year_b)
    if month_b < 10:
        month_b = '0' + str(month_b)
    else:
        month_b = str(month_b)
    if day_b < 10:
        day_b = '0' + str(day_b)
    else:
        day_b = str(day_b)

    return year_b, month_b, day_b, days, re_download, projects, ftype, all


def get_url_files(project='rv',year='2023',month='09',type='rib'):
    """
    :param project: rv=routeviews or ripe
    :param year:
    :param month:
    :param type: rib or updates
    :return: dict with collectors and files names
    """
    link_files = []
    link_downloads = {}
    if project=='rv':
        url = rv_url
        sleep_randon = False
        collectors = rv_collectors
        type_file = rv_type_file
        if type == 'rib':
            additional = 'RIBS/'
            s_file = 'rib'
        else:
            additional = 'UPDATES/'
            s_file = 'updates'
    else:
        url = ripe_url
        sleep_randon = True
        collectors = ripe_collectors
        type_file = ripe_type_file
        if type == 'rib':
            additional = ''
            s_file = 'bview'
        else:
            additional = ''
            s_file = 'updates'
    bar = progressbar.ProgressBar(max_value=len(collectors),redirect_stdout=True)
    print('Analysing the collectors!!! (', project,')')
    for i, c in enumerate(collectors):
        if not c.startswith('/'):
            c = '/' + c
        f_url = url + c + '/' + year + '.' + month + '/' + additional
        #print(f_url)
        try:
            response = urllib.request.urlopen(f_url)
            webContent = response.read().decode('UTF-8')
            soup = BeautifulSoup(webContent, 'html.parser')
            if sleep_randon:
                t = uniform(0.5, 2.)
                time.sleep(t)
        except:
            print('Error to access:', f_url)
            bar.update(i)
            continue
        for link in soup.findAll('a'):
            file = link.get('href')
            if file.endswith(type_file):
                link_files.append(file)
        link_downloads[c] = link_files
        link_files = []
        bar.update(i)
    bar.finish()
    return link_downloads


def get_caida_url_files(year='2023',month='09'):
    """
    :param year:
    :param month:
    :return: dict with collectors and files names
    """
    link_files = []
    link_downloads = {}
    url = caida_rel_url
    type_file = caida_type_file
    try:
        response = urllib.request.urlopen(url)
        webContent = response.read().decode('UTF-8')
        soup = BeautifulSoup(webContent, 'html.parser')
    except:
        print('Error to access:', url)
        return link_downloads
    for link in soup.findAll('a'):
        file = link.get('href')
        if file.endswith(type_file) and file.startswith((year+month)):
            return str(file)

    return ''


def select_url_files(files_names={}, day='29', all=False):
    """
    Select downloads per date to download
    :param files_names: a dictionary with collectors and files names from some month
    :param day: with day from month to download
    :param all: all files from that day (True) or only first file from date (False)
    :return: a dictionary with information to download files
    """
    files_download = {}
    files_to_download = []
    collectors = files_names.keys()
    bar = progressbar.ProgressBar(max_value=len(collectors), redirect_stdout=True)
    print('Analysing files from the collectors!!!')
    for i, c in enumerate(collectors):
        files = files_names[c]
        for f in files:
            name = f.split('.')
            if name[1].endswith(day):
                if not all:
                    files_to_download = [f]
                    break
                else:
                    files_to_download.append(f)

        files_download[c] = files_to_download
        files_to_download = []
        bar.update(i)

    bar.finish()
    return files_download


def download_selected_files(project='rv',type='rib',year='2023',month='09',files_names={}, re_download = False):
    """
    Download selected files
    :param project: with project files will be download
    :param type: files type updates or rib
    :param year:
    :param month:
    :param files_names: selected files
    :param re_download: download again if the files exists on file system
    :return: nothing, the files will be saved in the file system
    """
    folder = './'+type
    if not os.path.isdir(folder):
        os.mkdir(folder)
    folder = folder + '/' + project
    if not os.path.isdir(folder):
        os.mkdir(folder)
    collectors = files_names.keys()
    if project == 'rv':
        url = rv_url
        sleep_randon = False
        if type == 'rib':
            additional = 'RIBS/'
        else:
            additional = 'UPDATES/'
    elif project == 'ripe':
        url = ripe_url
        sleep_randon = True
        additional = ''
    else:
        print("Incorrect project:", project)
        return None
    bar = progressbar.ProgressBar(max_value=len(collectors), redirect_stdout=True)
    print('Download files from the collectors !!!(',project,')')
    for i, c in enumerate(collectors):
        files = files_names[c]
        collector = c.split('/')
        if c.startswith('/'):
            collector = collector[1]
        else:
            collector = collector[0]
        d_folder = folder + '/' + collector
        if not os.path.isdir(d_folder):
            os.mkdir(d_folder)
        if not c.startswith('/'):
            c = '/' + c
        f_url = url + c + '/' + year + '.' + month + '/' + additional
        for file in files:
            #d_url = f_url + '/' + file
            d_url = f_url + file
            local_file = d_folder + '/' + file
            if os.path.isfile(local_file) and re_download == False:
                site_size = urllib.request.urlopen(d_url)
                site_size = site_size.length
                local_size = os.path.getsize(local_file)
                if site_size == local_size:
                    continue
                print("File updated or with error, downloading file again,", local_file)
            #print(d_url)
            try:
                urllib.request.urlretrieve(d_url, local_file)
            except:
                print('Fail to download', d_url)
            if sleep_randon:
                t = uniform(0.5, 2.)
                time.sleep(t)
            bar.update(i)
    bar.finish()


def download_caida_files(caida_file=''):
    """
    Donwload select CAIDA relationship files
    :param caida_file: CAIDA file name
    :return: nothing, the file will be saved in the file system
    """
    if caida_file=='':
        print('Fail to download CAIDA: file.')
    folder = './caida'
    if not os.path.isdir(folder):
        os.mkdir(folder)
    f_url = caida_rel_url
    d_url = f_url + "/" + caida_file
    local_file = folder + '/' + caida_file
    if os.path.isfile(local_file) and re_download == False:
        site_size = urllib.request.urlopen(d_url)
        site_size = site_size.length
        local_size = os.path.getsize(local_file)
        if site_size == local_size:
            return None
    try:
        print("Downloading CAIDA files.")
        urllib.request.urlretrieve(d_url, local_file)
    except:
        print('Fail to download', d_url)


def next_date(year_b, month_b, day_b,days):
    """
    Compute some date with plus n days
    :param year_b:
    :param month_b:
    :param day_b:
    :param days:
    :return: year, month, day
    """
    begin = datetime(int(year_b), int(month_b), int(day_b), 0, 0, 0, tzinfo=timezone.utc)
    end_time = begin + timedelta(days=days)
    year = end_time.year
    month = end_time.month
    day = end_time.day
    return year, month, day


def download_files(project=['rv','ripe','caida'],type='rib',year_b='2023',month_b='09', day_b = '29',
                   num_days = 1, all= False, re_download = False):
    """
    Select and download files based on type, project and date between start date and n days after
    :param project:
    :param type:
    :param year_b:
    :param month_b:
    :param day_b:
    :param num_days:
    :param all:
    :param re_download:
    :return:  nothing, the files will be saved in the file system
    """
    d=0
    link_downloads = {}
    ld_rv = {}
    ld_ripe = {}
    m = '00'
    while d < num_days:
        year, month, day = next_date(year_b, month_b, day_b, d)
        year = str(year)
        if month < 10:
            month = '0' + str(month)
        else:
            month = str(month)
        if day < 10:
            day = '0' + str(day)
        else:
            day = str(day)
        print('Analysing '+year+"/"+month+"/"+day)
        for p in project:

            if p != 'caida':
                if p == 'rv':
                    if m != month:
                        ld_rv = get_url_files(project=p, year=year, month=month, type=type)
                    link_downloads = ld_rv
                elif p == 'ripe':
                    if m != month:
                        ld_ripe = get_url_files(project=p, year=year, month=month, type=type)
                    link_downloads = ld_ripe
                link_downloads = select_url_files(files_names=link_downloads, day=day, all=all)
                download_selected_files(project=p, type=type, year=year, month=month,
                                        files_names=link_downloads, re_download=re_download)
            elif p == 'caida':
                if m != month:
                    caida_file = get_caida_url_files(year=year, month=month)
                    download_caida_files(caida_file)
                continue

            #link_downloads = ''
        m = month
        d += 1


if __name__ == '__main__':
    year_b, month_b, day_b, days, re_download, projects, ftype, all = arguments()
    download_files(project=projects, type=ftype, year_b=year_b, month_b=month_b, day_b = day_b, num_days=days,
                   all=all, re_download=re_download)

