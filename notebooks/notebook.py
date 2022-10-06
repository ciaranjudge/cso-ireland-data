# %%
from datetime import datetime

from cso_ireland_data import CSODataSession

cso = CSODataSession(request_params={"verify": False})

# monthly_cpi(verify=False)
lt = cso.life_table()

lt


# %%
lr = cso.live_register(start=datetime(2010,1,1))
lr
# %%
toc = cso.get_toc()
# %%
