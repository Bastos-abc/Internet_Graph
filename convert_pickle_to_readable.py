import os
import pickle
import json
from config import out_folder
convert_json = True
convert_vfv = True
r_folder = './readable/'
files = os.listdir(out_folder)
pickle_files = []
vfv_np_files = []

for f in files:
    if not 'period' in f:
        if f.endswith('_vfv.pickle'):
            vfv_np_files.append(f)
        elif f.endswith('.pickle'):
            pickle_files.append(f)

if not os.path.isdir(r_folder):
    os.mkdir(r_folder)

if convert_json:
    for f in pickle_files:
        json_file = r_folder + f.split('.')[0] + '.json'
        print("converting", out_folder + f)
        if not os.path.isfile(json_file):
            in_file = open(out_folder + f, 'rb')
            tmp = pickle.load(in_file)
            out_file = open(json_file, 'w')
            json.dump(tmp, out_file)
            in_file.close()
            out_file.close()

if convert_vfv:
    for f in vfv_np_files:
        in_file = open(out_folder + f, 'rb')
        out_file = r_folder + f.split('.')[0] + '.txt'
        vfv = pickle.load(in_file)
        txt_f = open(out_file,'wt')
        for x in vfv:
            if len(x) == 2:
                print(x[0], '|', x[1], file=txt_f)
            else:
                print(x)
        txt_f.close()
