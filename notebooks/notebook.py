# # %%
# from datetime import datetime

# from zmq import REP

# from cso_ireland_data import CSODataSession

# cso = CSODataSession(request_params={"verify": False})

# # monthly_cpi(verify=False)
# lt = cso.life_table("px", "all")
# cso.life_table()

# lt


# # %%
# lr = cso.live_register(start=datetime(2010,1,1))
# lr
# # %%
# toc = cso.get_toc()
# # %%
# import certifi
# import requests

# try:
#     print('Checking connection to Github...')
#     test = requests.get('https://api.github.com')
#     print('Connection to Github OK.')
# except requests.exceptions.SSLError as err:
#     print('SSL Error. Adding custom certs to Certifi store...')
#     cafile = certifi.where()
#     with open('dsp_root_ca.cer', 'rb') as infile:
#         customca = infile.read()
#     with open(cafile, 'ab') as outfile:
#         outfile.write(customca)
#     print('That might have worked.')
# %%
from requests import Session
from twine.commands.upload import upload
from twine.settings import Settings
from pathlib import Path
import git

repo = git.Repo(".", search_parent_directories=True)
REPO_ROOT = Path(repo.working_tree_dir)

s = Session()
s.verify = False
cacert_file = Path.home() / "certs" / "FGT-CASSL-22-27.pem"
settings = Settings(cacert=str(cacert_file))
dist_folder = REPO_ROOT / "dist"
dists = [str(f) for f in dist_folder.glob("*")]
upload(upload_settings=settings, dists=dists)


# %%
USER_HOME = Path.home()
USER_HOME
# %%
cert_folder = Path.home() / "certs"
import subprocess
for f in cert_folder.glob("*.crt"):
    f_stem = f"{cert_folder}\\{f.stem}"
    print(f_stem)
    process = subprocess.run(f"openssl x509 -in {f_stem}.crt -out {f_stem}.pem")
    print(process)


# %%
