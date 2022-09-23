# %%
from datetime import datetime

import cso_data as cso

# monthly_cpi(verify=False)
lt = cso.life_table()

lt


# %%
lr = cso.live_register(start=datetime(2010,1,1))
lr
# %%
toc = cso.get_toc()
# %%
