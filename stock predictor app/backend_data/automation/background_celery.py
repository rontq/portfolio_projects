from celery import shared_task, chain
from datetime import datetime
from data_fetch_store import upd_data_fetch
from data_fetch_store import upd_index_sector_calc
from data_fetch_store import upd_index_subsector_calc
from data_fetch_store import upd_vol_sma_subsector_calc
from data_fetch_store import upd_company_weight

# Celery task to update the database
@shared_task
def update_database_task(force_update: bool = False, start_date: datetime.date = None):
    upd_data_fetch.main(force_update=force_update, start_date=start_date)
    return "Database Updated"

# Celery task to update the sector index
@shared_task
def update_sector_index_task(force_update: bool = False, start_date: datetime.date = None):
    upd_index_sector_calc.calculate_sector(force_update=force_update, start_date=start_date)
    return "Sector Index Updated"

# Celery task to update the subsector index
@shared_task
def update_subsector_index_task(force_update: bool = False, start_date: datetime.date = None):
    upd_index_subsector_calc.calculate_subsector(force_update=force_update, start_date=start_date)
    return "Subsector Index Updated"

# Celery task to update the subsector volatility and SMA
@shared_task
def update_subsector_vol_sma_task(force_update: bool = False, start_date: datetime.date = None):
    upd_vol_sma_subsector_calc.calculate_vol_sma(force_update=force_update, start_date=start_date)
    return "Subsector Volatility and SMA Updated"

# Celery task to update company weight
@shared_task
def update_company_weight_task(force_update: bool = False, start_date: datetime.date = None):
    upd_company_weight.calculate_company_weight(force_update=force_update, start_date=start_date)
    return "Company Weight Updated"

# Chain all the tasks together
def update_all_tasks(force_update: bool = False, start_date: datetime.date = None):
    task_chain = chain(
        update_database_task.s(force_update, start_date),
        update_sector_index_task.s(force_update, start_date),
        update_subsector_index_task.s(force_update, start_date),
        update_subsector_vol_sma_task.s(force_update, start_date),
        update_company_weight_task.s(force_update, start_date)
    )
    task_chain()
