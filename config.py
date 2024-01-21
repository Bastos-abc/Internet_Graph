#Variables to adjust the code executino
# number of maximum competitors threads
n_threads = 2
img_folder = './img/'
# Folder to save computed information
out_folder = './info/'
# Where temp files will be saved
tmp_dir = './tmp/'
# First day to analyse
initial_date = '20230101'
# how many days after first
days = '300'
# Projects to analyse
# Only Route Views and CAIDA relationships
projects = ['rv', 'caida']


# https://archive.routeviews.org/<colector>/<YYYY.MM>/<RIBS/UPDATES>/<rib/updates>.<YYYYMMDD>.<hhmm>.bz2
rv_url = "https://archive.routeviews.org"
rv_type_file = '.bz2'

rv_collectors = ['/bgpdata', '/decix.jhb/bgpdata', '/pacwave.lax/bgpdata', '/route-views2.saopaulo/bgpdata',
                '/route-views3/bgpdata', '/route-views4/bgpdata', '/route-views5/bgpdata', '/route-views6/bgpdata',
                '/route-views.amsix/bgpdata', '/route-views.bdix/bgpdata', '/route-views.bknix/bgpdata',
                '/route-views.chicago/bgpdata', '/route-views.chile/bgpdata', '/route-views.eqix/bgpdata',
                '/route-views.flix/bgpdata', '/route-views.fortaleza/bgpdata', '/route-views.gixa/bgpdata',
                '/route-views.gorex/bgpdata', '/route-views.isc/bgpdata', '/route-views.jinx/bgpdata',
                '/route-views.kixp/bgpdata', '/route-views.linx/bgpdata', '/route-views.mwix/bgpdata',
                '/route-views.napafrica/bgpdata', '/route-views.nwax/bgpdata', '/route-views.ny/bgpdata',
                '/route-views.perth/bgpdata', '/route-views.peru/bgpdata', '/route-views.phoix/bgpdata',
                '/route-views.rio/bgpdata', '/route-views.saopaulo/bgpdata', '/route-views.sfmix/bgpdata',
                '/route-views.sg/bgpdata', '/route-views.siex/bgpdata', '/route-views.soxrs/bgpdata',
                '/route-views.sydney/bgpdata', '/route-views.telxatl/bgpdata', '/route-views.uaeix/bgpdata',
                '/route-views.wide/bgpdata']

#https://data.ris.ripe.net/<colector>/<YYYY.MM>/<bview/updates>.YYYYMMDD>.<hhmm>.gz
ripe_url = "https://data.ris.ripe.net"
ripe_type_file = '.gz'

ripe_collectors = ['rrc00', 'rrc01', 'rrc03', 'rrc04', 'rrc05', 'rrc06', 'rrc07', 'rrc10', 'rrc11', 'rrc12', 'rrc13',
                  'rrc14', 'rrc15', 'rrc16', 'rrc18', 'rrc19', 'rrc20', 'rrc21', 'rrc22', 'rrc23', 'rrc24', 'rrc25',
                  'rrc26']

caida_rel_url = "https://publicdata.caida.org/datasets/as-relationships/serial-2/"
caida_type_file = '.bz2'

#Files to download to ROV
DEFAULT_IRR_URLS = [
        # RADB
        'ftp://ftp.radb.net/radb/dbase/altdb.db.gz',
        #'ftp://ftp.radb.net/radb/dbase/aoltw.db.gz',
        #'ftp://ftp.radb.net/radb/dbase/arin-nonauth.db.gz',
        #'ftp://ftp.radb.net/radb/dbase/arin.db.gz',
        'ftp://ftp.radb.net/radb/dbase/bboi.db.gz',
        'ftp://ftp.radb.net/radb/dbase/bell.db.gz',
        'ftp://ftp.radb.net/radb/dbase/canarie.db.gz',
        #'ftp://ftp.radb.net/radb/dbase/easynet.db.gz',
        'ftp://ftp.radb.net/radb/dbase/jpirr.db.gz',
        'ftp://ftp.radb.net/radb/dbase/level3.db.gz',
        'ftp://ftp.radb.net/radb/dbase/nestegg.db.gz',
        'ftp://ftp.radb.net/radb/dbase/nttcom.db.gz',
        'ftp://ftp.radb.net/radb/dbase/openface.db.gz',
        #'ftp://ftp.radb.net/radb/dbase/ottix.db.gz',
        'ftp://ftp.radb.net/radb/dbase/panix.db.gz',
        'ftp://ftp.radb.net/radb/dbase/radb.db.gz',
        'ftp://ftp.radb.net/radb/dbase/reach.db.gz',
        'ftp://ftp.radb.net/radb/dbase/rgnet.db.gz',
        #'ftp://ftp.radb.net/radb/dbase/risq.db.gz',
        #'ftp://ftp.radb.net/radb/dbase/rogers.db.gz',
        'ftp://ftp.radb.net/radb/dbase/tc.db.gz',
        # RIRs
        #'ftp://ftp.arin.net/pub/rr/arin-nonauth.db.gz',
        'ftp://ftp.arin.net/pub/rr/arin.db.gz',
        'ftp://ftp.afrinic.net/pub/dbase/afrinic.db.gz',
        'ftp://ftp.apnic.net/pub/apnic/whois/apnic.db.route.gz',
        'https://ftp.lacnic.net/lacnic/irr/lacnic.db.gz',
        'ftp://ftp.ripe.net/ripe/dbase/split/ripe-nonauth.db.route.gz',
        'ftp://ftp.ripe.net/ripe/dbase/split/ripe-nonauth.db.route6.gz',
        'ftp://ftp.ripe.net/ripe/dbase/split/ripe.db.route.gz',
        'ftp://ftp.ripe.net/ripe/dbase/split/ripe.db.route6.gz',
        ]
DEFAULT_RPKI_URLS = [
        'https://rpki.gin.ntt.net/api/export.json'
        ]
RPKI_ARCHIVE_URLS = [
        'https://ftp.ripe.net/ripe/rpki/afrinic.tal/{year:04d}/{month:02d}/{day:02d}/roas.csv',
        'https://ftp.ripe.net/ripe/rpki/apnic.tal/{year:04d}/{month:02d}/{day:02d}/roas.csv',
        'https://ftp.ripe.net/ripe/rpki/arin.tal/{year:04d}/{month:02d}/{day:02d}/roas.csv',
        'https://ftp.ripe.net/ripe/rpki/lacnic.tal/{year:04d}/{month:02d}/{day:02d}/roas.csv',
        'https://ftp.ripe.net/ripe/rpki/ripencc.tal/{year:04d}/{month:02d}/{day:02d}/roas.csv',
        ]
