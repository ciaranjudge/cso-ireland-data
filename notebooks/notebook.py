# %%
import cso_data as cso

# monthly_cpi(verify=False)
lr = cso.live_register(verify=False)
lr = lr.loc[:,
    ["Persons on the Live Register"]
]
lr
# %%
