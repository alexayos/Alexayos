# """
# Dealroom Data Importer
# ======================

# Массовый экспорт из Dealroom API: 3.4M компаний, 120+ полей

# Примеры использования:
#     # Инициализация
#     importer = DealroomImporter(API_KEY)
    
#     # Bulk: все 3.4M компаний (~10-12 часов)
#     df = importer.bulk_export()
    
#     # Bulk: первые 10K компаний (~10-15 минут)
#     df = importer.bulk_export(max_companies=10000)
    
#     # Regular: до 10K компаний
#     df = importer.import_companies(n=5000)
    
#     # Funding данные
#     fundings = importer.import_fundings(company_ids)
# """

# import time
# import pandas as pd
# import requests
# import base64
# from typing import List, Optional
# import logging
# import json
# from datetime import datetime

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)


# class DealroomImporter:
#     """Dealroom API data importer"""
    
#     def __init__(self, api_key: str):
#         self.api_key = api_key
#         self.base_url = "https://api.dealroom.co/api/v1"
#         self.headers = {
#             "Accept": "application/json",
#             "Content-Type": "application/json",
#             "Authorization": "Basic " + base64.b64encode(f"{api_key}:".encode()).decode()
#         }
    
#     def get_all_company_fields(self) -> str:
#         """Все доступные поля для компаний (100+ полей)"""
#         return (
#             "id,name,deleted,path,tagline,about,type,url,"
#             "website_url,twitter_url,facebook_url,linkedin_url,google_url,"
#             "crunchbase_url,angellist_url,"
#             "playmarket_app_id,appstore_app_id,"
#             "images,"
#             "employees,employees_latest,"
#             "industries,sub_industries,corporate_industries,service_industries,"
#             "technologies,income_streams,revenues,client_focus,"
#             "growth_stage,"
#             "hq_locations,legal_entities,"
#             "tags,ownerships,"
#             "launch_year,launch_month,closing_year,closing_month,"
#             "has_promising_founder,has_strong_founder,has_super_founder,"
#             "total_funding,total_funding_currency,total_funding_source,"
#             "last_funding,last_funding_source,last_funding_date,"
#             "company_status,"
#             "last_updated,last_updated_utc,created_utc,"
#             "employees_chart,website_traffic_estimates_chart,"
#             "app_downloads_ios_chart,app_downloads_android_chart,"
#             "app_downloads_ios_incremental_chart,app_downloads_android_incremental_chart,"
#             "website_traffic_3_months_growth_unique,"
#             "website_traffic_3_months_growth_percentile,"
#             "website_traffic_3_months_growth_relative,"
#             "website_traffic_3_months_growth_delta,"
#             "website_traffic_6_months_growth_unique,"
#             "website_traffic_6_months_growth_percentile,"
#             "website_traffic_6_months_growth_relative,"
#             "website_traffic_6_months_growth_delta,"
#             "website_traffic_12_months_growth_unique,"
#             "website_traffic_12_months_growth_percentile,"
#             "website_traffic_12_months_growth_relative,"
#             "website_traffic_12_months_growth_delta,"
#             "app_3_months_growth_unique,"
#             "app_3_months_growth_percentile,"
#             "app_3_months_growth_relative,"
#             "app_6_months_growth_unique,"
#             "app_6_months_growth_percentile,"
#             "app_6_months_growth_relative,"
#             "app_12_months_growth_unique,"
#             "app_12_months_growth_percentile,"
#             "app_12_months_growth_relative,"
#             "employee_3_months_growth_unique,"
#             "employee_3_months_growth_percentile,"
#             "employee_3_months_growth_relative,"
#             "employee_3_months_growth_delta,"
#             "employee_6_months_growth_unique,"
#             "employee_6_months_growth_percentile,"
#             "employee_6_months_growth_relative,"
#             "employee_6_months_growth_delta,"
#             "employee_12_months_growth_unique,"
#             "employee_12_months_growth_percentile,"
#             "employee_12_months_growth_relative,"
#             "employee_12_months_growth_delta,"
#             "innovation_corporate_rank,"
#             "kpi_summary,"
#             "team,investors,fundings,"
#             "limited_partner_investments,known_limited_partners,"
#             "job_openings,"
#             "tech_stack_predictleads,"
#             "sustainable_development_goals,"
#             "data_type,"
#             "pic_number,patents_count,"
#             "dealroom_signal"
#         )
    
#     def get_all_funding_fields(self) -> str:
#         return (
#             "id,year,month,day,amount,amount_source,currency,round,round_source,"
#             "valuation,valuation_source,valuation_generated_min,valuation_generated_max,"
#             "amount_eur_million,amount_usd_million,is_verified,investors,lead_investors,"
#             "transaction_multiples,source,source_round,notes"
#         )
    
#     def import_companies(self, n: int, keyword: str = None) -> pd.DataFrame:
#         """
#         Regular export via /companies (up to 10K companies)
        
#         Args:
#             n: Number of companies (max 10,000 due to API offset limit)
#             keyword: Optional keyword filter
#         """
#         if n > 10000:
#             logger.warning("Regular endpoint limited to 10K. Use bulk_export() for more.")
#             n = 10000
        
#         all_companies = []
#         offset = 0
#         batch_size = 100
        
#         print(f"📊 Importing {n:,} companies via /companies...")
        
#         while sum(len(df) for df in all_companies) < n:
#             remaining = n - sum(len(df) for df in all_companies)
#             current_limit = min(batch_size, remaining)
            
#             payload = {
#                 "fields": self.get_all_company_fields(),
#                 "limit": current_limit,
#                 "offset": offset
#             }
            
#             if keyword:
#                 payload["keyword"] = keyword
#                 payload["keyword_type"] = "name"
            
#             try:
#                 response = requests.post(
#                     f"{self.base_url}/companies",
#                     json=payload,
#                     headers=self.headers,
#                     timeout=60
#                 )
#                 response.raise_for_status()
#                 data = response.json()
#                 items = data.get("items", data if isinstance(data, list) else [])
                
#                 if not items:
#                     break
                
#                 df_batch = pd.json_normalize(items, sep="__")
#                 all_companies.append(df_batch)
#                 offset += len(items)
                
#                 total = sum(len(df) for df in all_companies)
#                 print(f"\rProgress: {total:,}/{n:,} ({total/n*100:.1f}%)", end="", flush=True)
                
#                 if len(items) < current_limit:
#                     break
                
#                 time.sleep(1.0)
                
#             except Exception as e:
#                 logger.error(f"Error: {e}")
#                 break
        
#         if all_companies:
#             final_df = pd.concat(all_companies, ignore_index=True)
#             if 'id' in final_df.columns:
#                 final_df = final_df.drop_duplicates(subset=['id'], keep='first')
#             print(f"\n✅ Done: {len(final_df):,} companies")
#             return final_df
        
#         return pd.DataFrame()
    
#     def bulk_export(self, 
#                     max_companies: Optional[int] = None,
#                     save_every: int = 100000,
#                     output_prefix: str = "dealroom_bulk",
#                     resume_next_page_id: str = None) -> pd.DataFrame:
#         """
#         Bulk export via /companies/bulk (up to 3.4M companies)
        
#         Args:
#             max_companies: Max number of companies (None = all available)
#             save_every: Checkpoint frequency (default: 100K, auto-saves as CSV.GZ for >100K rows)
#             output_prefix: Output filename prefix
#             resume_next_page_id: Resume with this next_page_id (from checkpoint .json file)
#         """
#         import glob
#         import os
#         import time as time_module
        
#         all_companies = []
#         total_fetched = 0
#         checkpoint_counter = 0
#         next_page_id = resume_next_page_id  # Resume from provided next_page_id
#         page_number = 0
#         page_size = 100
#         retry_count = 0
#         max_retries = 5
#         base_delay = 2.0
#         last_request_time = None  # Track time of last request for 10-min warning
        
#         # Load existing checkpoint if resuming
#         if resume_next_page_id:
#             # Find checkpoint file corresponding to the resume token
            
#             # Try to find the JSON file to get the exact checkpoint number
#             json_files = glob.glob(f"{output_prefix}_checkpoint_*.json")
#             checkpoint_number = None
            
#             # Find which JSON has this next_page_id
#             for json_file in json_files:
#                 try:
#                     with open(json_file, 'r') as f:
#                         data = json.load(f)
#                         if data.get("next_page_id") == resume_next_page_id:
#                             # Extract number from filename
#                             checkpoint_number = int(json_file.split('_')[-1].split('.')[0])
#                             print(f"📂 Found matching checkpoint: {checkpoint_number:,} companies")
#                             break
#                 except:
#                     continue
            
#             # Find corresponding data file - ONLY for the exact checkpoint number
#             if checkpoint_number:
#                 possible_files = [
#                     f"{output_prefix}_checkpoint_{checkpoint_number}.csv.gz",
#                     f"{output_prefix}_checkpoint_{checkpoint_number}.csv",
#                     f"{output_prefix}_checkpoint_{checkpoint_number}.xlsx"
#                 ]
#                 # Filter to only existing files
#                 all_checkpoints = [f for f in possible_files if os.path.exists(f)]
#             else:
#                 # No matching JSON found - cannot resume safely
#                 logger.error(f"❌ No checkpoint JSON found for next_page_id. Cannot resume safely.")
#                 logger.error(f"💡 Start a new export or provide correct next_page_id")
#                 return pd.DataFrame()
            
#             if all_checkpoints:
#                 # Use first found checkpoint (they should all be the same number)
#                 latest_checkpoint = all_checkpoints[0]
#                 print(f"📂 Loading checkpoint: {latest_checkpoint}")
                
#                 try:
#                     # Try different formats
#                     if latest_checkpoint.endswith('.csv.gz'):
#                         df_checkpoint = pd.read_csv(latest_checkpoint, compression='gzip')
#                     elif latest_checkpoint.endswith('.csv'):
#                         df_checkpoint = pd.read_csv(latest_checkpoint)
#                     elif latest_checkpoint.endswith('.xlsx'):
#                         # Try Excel first
#                         try:
#                             df_checkpoint = pd.read_excel(latest_checkpoint, engine='openpyxl')
#                         except:
#                             # If fails, might be CSV with .xlsx extension (old bug)
#                             try:
#                                 df_checkpoint = pd.read_csv(latest_checkpoint)
#                                 logger.warning("Checkpoint .xlsx is actually CSV format")
#                             except:
#                                 # Last resort: try reading as text
#                                 df_checkpoint = pd.read_csv(latest_checkpoint, engine='python', on_bad_lines='skip')
#                     else:
#                         logger.error(f"Unknown checkpoint format: {latest_checkpoint}")
#                         return pd.DataFrame()
                    
#                     all_companies.append(df_checkpoint)
#                     total_fetched = len(df_checkpoint)
#                     print(f"✅ Resumed from {total_fetched:,} companies")
#                     print(f"🔑 Using next_page_id to continue...\n")
                    
#                 except Exception as e:
#                     logger.error(f"Failed to load checkpoint: {e}")
#                     print(f"💡 Try manually loading: df = pd.read_csv('{latest_checkpoint}')")
#                     return pd.DataFrame()
#             else:
#                 print("⚠️ No checkpoint found, starting from scratch\n")
        
#         print(f"🚀 Bulk export via /companies/bulk")
#         print(f"📊 Available: 3,427,870 companies")
#         print(f"🎯 Limit: {max_companies:,}" if max_companies else "🎯 Limit: ALL")
#         print(f"💾 Checkpoint: every {save_every:,} (CSV.GZ for files >100K rows)")
#         print(f"⏱️ Delay: {base_delay}s + exponential backoff on errors\n")
        
#         while True:
#             if max_companies and total_fetched >= max_companies:
#                 break
            
#             # ⏱️ Check if more than 9 minutes passed since last request (warn about 10-min limit)
#             if last_request_time:
#                 elapsed_minutes = (time_module.time() - last_request_time) / 60
#                 if elapsed_minutes > 9:
#                     logger.warning(f"⚠️ {elapsed_minutes:.1f} minutes since last request! next_page_id expires after 10 min!")
            
#             page_number += 1
#             payload = {"fields": self.get_all_company_fields(), "limit": page_size}
#             if next_page_id:
#                 payload["next_page_id"] = next_page_id
            
#             try:
#                 last_request_time = time_module.time()  # Track request time
                
#                 response = requests.post(
#                     f"{self.base_url}/companies/bulk",
#                     json=payload,
#                     headers=self.headers,
#                     timeout=90
#                 )
                
#                 # Handle server errors with retry (max total delay: 62 seconds < 10 min)
#                 if response.status_code in [520, 502, 503, 504]:
#                     retry_count += 1
#                     if retry_count > max_retries:
#                         logger.error(f"Max retries ({max_retries}) reached. Stopping.")
#                         break
                    
#                     wait_time = base_delay * (2 ** (retry_count - 1))
#                     logger.warning(f"Server error {response.status_code}. Retry {retry_count}/{max_retries} in {wait_time:.0f}s...")
#                     logger.info(f"⏱️ Total retry time so far: {sum([base_delay * (2**i) for i in range(retry_count)]):.0f}s (limit: 10 min = 600s)")
#                     time.sleep(wait_time)
#                     continue
                
#                 if response.status_code != 200:
#                     logger.error(f"Error {response.status_code}: {response.text[:200]}")
#                     break
                
#                 # Reset retry counter on success
#                 retry_count = 0
                
#                 data = response.json()
#                 items = data.get("items", [])
#                 next_page_id = data.get("next_page_id")
                
#                 if not items:
#                     break
                
#                 df_batch = pd.json_normalize(items, sep="__")
#                 all_companies.append(df_batch)
#                 total_fetched += len(df_batch)
#                 checkpoint_counter += len(df_batch)
                
#                 print(f"\rPage {page_number:,} | Fetched: {len(df_batch):3} | Total: {total_fetched:,}", end="", flush=True)
                
#                 # Checkpoint
#                 if checkpoint_counter >= save_every:
#                     logger.info(f"💾 Saving checkpoint at {total_fetched:,} companies...")
#                     checkpoint_start_time = time_module.time()
                    
#                     # Fix FutureWarning: handle empty DataFrames properly
#                     if all_companies:
#                         combined_df = pd.concat(all_companies, ignore_index=True, sort=False)
#                         if 'id' in combined_df.columns:
#                             combined_df = combined_df.drop_duplicates(subset=['id'], keep='first')
#                     else:
#                         combined_df = pd.DataFrame()
                    
#                     # Save checkpoint (auto-selects Excel or CSV.GZ)
#                     checkpoint_file = f"{output_prefix}_checkpoint_{len(combined_df)}.xlsx"
#                     safe_to_excel(combined_df, checkpoint_file)
                    
#                     # Save next_page_id to resume later
#                     if next_page_id:
#                         json_file = f"{output_prefix}_checkpoint_{len(combined_df)}.json"
#                         with open(json_file, 'w') as f:
#                             json.dump({
#                                 "next_page_id": next_page_id, 
#                                 "total": len(combined_df),
#                                 "timestamp": datetime.now().isoformat()
#                             }, f, indent=2)
#                         print(f"🔑 Resume token: {json_file}")
                    
#                     checkpoint_duration = time_module.time() - checkpoint_start_time
#                     logger.info(f"✅ Checkpoint saved in {checkpoint_duration:.1f}s")
                    
#                     # Warn if checkpoint took too long (approaching 10-min limit)
#                     if checkpoint_duration > 300:  # 5 minutes
#                         logger.warning(f"⚠️ Checkpoint took {checkpoint_duration/60:.1f} min! Risk of next_page_id expiration!")
                    
#                     checkpoint_counter = 0
#                     last_request_time = time_module.time()  # Reset timer after checkpoint
                
#                 if not next_page_id:
#                     break
                
#                 time.sleep(base_delay)
                
#             except Exception as e:
#                 logger.error(f"Exception: {e}")
#                 retry_count += 1
#                 if retry_count > max_retries:
#                     break
#                 wait_time = base_delay * (2 ** (retry_count - 1))
#                 logger.warning(f"Retry {retry_count}/{max_retries} in {wait_time:.0f}s...")
#                 time.sleep(wait_time)
        
#         if all_companies:
#             logger.info(f"\n💾 Creating final file...")
#             final_df = pd.concat(all_companies, ignore_index=True, sort=False)
#             if 'id' in final_df.columns:
#                 final_df = final_df.drop_duplicates(subset=['id'], keep='first')
            
#             timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
#             final_file = f"{output_prefix}_{len(final_df)}_{timestamp}.xlsx"
#             safe_to_excel(final_df, final_file)
            
#             print(f"\n\n✅ Done: {len(final_df):,} companies → {final_file}")
#             print(f"📄 Pages: {page_number:,}")
#             print(f"⏱️ Average delay between requests: {base_delay}s")
#             print(f"🔄 Max retry delay: {sum([base_delay * (2**i) for i in range(max_retries)]):.0f}s (< 10 min limit)")
#             return final_df
#         else:
#             logger.warning("No companies fetched!")
#             return pd.DataFrame()
    
#     def import_fundings(self, company_ids: List[str]) -> pd.DataFrame:
#         """
#         Import funding data for companies
        
#         Args:
#             company_ids: List of company IDs
#         """
#         all_fundings = []
#         total = len(company_ids)
        
#         print(f"📊 Importing fundings for {total:,} companies...")
        
#         for i, company_id in enumerate(company_ids, 1):
#             try:
#                 response = requests.get(
#                     f"{self.base_url}/companies/{company_id}/fundings",
#                     headers=self.headers,
#                     params={"fields": self.get_all_funding_fields()},
#                     timeout=30
#                 )
#                 response.raise_for_status()
#                 items = response.json().get("items", [])
                
#                 if items:
#                     df_batch = pd.json_normalize(items, sep="__")
#                     df_batch['company_id'] = company_id
#                     all_fundings.append(df_batch)
                
#                 if i % 100 == 0:
#                     print(f"\rProgress: {i}/{total} ({i/total*100:.1f}%)", end="", flush=True)
                
#                 time.sleep(0.5 if i % 10 else 1.0)
                
#             except Exception as e:
#                 logger.error(f"Error for {company_id}: {e}")
        
#         if all_fundings:
#             result = pd.concat(all_fundings, ignore_index=True)
#             print(f"\n✅ Done: {len(result):,} funding rounds")
#             return result
#         return pd.DataFrame()


# def safe_to_excel(df: pd.DataFrame, filename: str):
#     """Сохранение в Excel/CSV (auto-detect для больших файлов)"""
#     # Для файлов >100K строк используем CSV (быстрее и надёжнее)
#     if len(df) > 100000:
#         csv_filename = filename.replace('.xlsx', '.csv.gz')
#         df.to_csv(csv_filename, index=False, compression='gzip')
#         logger.info(f"💾 Сохранено в CSV.GZ: {csv_filename} ({len(df):,} rows)")
#         return
    
#     # Для маленьких файлов пробуем Excel
#     try:
#         with pd.ExcelWriter(filename, engine='xlsxwriter', 
#                            engine_kwargs={'options': {
#                                'strings_to_urls': False,
#                                'use_zip64': True
#                            }}) as writer:
#             df.to_excel(writer, index=False, sheet_name='Companies')
#         logger.info(f"💾 Сохранено: {filename}")
#     except Exception as e:
#         # Fallback на CSV
#         logger.warning(f"Excel failed: {e}. Saving as CSV.GZ...")
#         csv_filename = filename.replace('.xlsx', '.csv.gz')
#         df.to_csv(csv_filename, index=False, compression='gzip')
#         logger.info(f"💾 Сохранено в CSV.GZ: {csv_filename}")


# # ============================================================================
# # ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ
# # ============================================================================

# if __name__ == "__main__":
#     # API ключ
#     API_KEY = "f353c6bd59a492aaa4eac73f84aa7c9cf30cc7fd"
    
#     # Инициализация
#     importer = DealroomImporter(API_KEY)
#     print("✅ Importer ready")
    
#     # ========================================================================
#     # Пример 1: BULK EXPORT - все компании (до 3.4M)
#     # ========================================================================
#     df = importer.bulk_export()
    
#     # ========================================================================
#     # Пример 2: BULK EXPORT - первые 10K компаний (~20-30 минут)
#     # ========================================================================
#     # df = importer.bulk_export(max_companies=10000)
    
#     # ========================================================================
#     # Пример 3: RESUME прерванного экспорта
#     # ========================================================================
#     # import json
#     # with open("dealroom_bulk_checkpoint_420000.json", "r") as f:
#     #     checkpoint = json.load(f)
#     #     resume_token = checkpoint["next_page_id"]
#     # df = importer.bulk_export(resume_next_page_id=resume_token)
    
#     # ========================================================================
#     # Пример 4: REGULAR EXPORT - до 10K компаний
#     # ========================================================================
#     # df = importer.import_companies(n=5000)
#     # if not df.empty:
#     #     timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
#     #     filename = f"dealroom_regular_{len(df)}_{timestamp}.xlsx"
#     #     safe_to_excel(df, filename)
    
#     # ========================================================================
#     # Пример 5: FUNDING DATA
#     # ========================================================================
#     # company_ids = df['id'].astype(str).tolist()[:100]
#     # fundings = importer.import_fundings(company_ids)
#     # if not fundings.empty:
#     #     timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
#     #     filename = f"dealroom_fundings_{len(fundings)}_{timestamp}.xlsx"
#     #     safe_to_excel(fundings, filename)


import os
import time
import json
import base64
import logging
import requests
import pandas as pd
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger("dealroom")


class DealroomImporter:

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.dealroom.co/api/v1"
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Basic " + base64.b64encode(f"{api_key}:".encode()).decode()
        }

    def get_all_company_fields(self) -> str:
        return (
            "id,name,deleted,path,tagline,about,type,url,"
            "website_url,twitter_url,facebook_url,linkedin_url,google_url,"
            "crunchbase_url,angellist_url,"
            "playmarket_app_id,appstore_app_id,"
            "images,"
            "employees,employees_latest,"
            "industries,sub_industries,corporate_industries,service_industries,"
            "technologies,income_streams,revenues,client_focus,"
            "growth_stage,"
            "hq_locations,legal_entities,"
            "tags,ownerships,"
            "launch_year,launch_month,closing_year,closing_month,"
            "has_promising_founder,has_strong_founder,has_super_founder,"
            "total_funding,total_funding_currency,total_funding_source,"
            "last_funding,last_funding_source,last_funding_date,"
            "company_status,"
            "last_updated,last_updated_utc,created_utc,"
            "employees_chart,website_traffic_estimates_chart,"
            "app_downloads_ios_chart,app_downloads_android_chart,"
            "app_downloads_ios_incremental_chart,app_downloads_android_incremental_chart,"
            "website_traffic_3_months_growth_unique,"
            "website_traffic_3_months_growth_percentile,"
            "website_traffic_3_months_growth_relative,"
            "website_traffic_3_months_growth_delta,"
            "website_traffic_6_months_growth_unique,"
            "website_traffic_6_months_growth_percentile,"
            "website_traffic_6_months_growth_relative,"
            "website_traffic_6_months_growth_delta,"
            "website_traffic_12_months_growth_unique,"
            "website_traffic_12_months_growth_percentile,"
            "website_traffic_12_months_growth_relative,"
            "website_traffic_12_months_growth_delta,"
            "app_3_months_growth_unique,"
            "app_3_months_growth_percentile,"
            "app_3_months_growth_relative,"
            "app_6_months_growth_unique,"
            "app_6_months_growth_percentile,"
            "app_6_months_growth_relative,"
            "app_12_months_growth_unique,"
            "app_12_months_growth_percentile,"
            "app_12_months_growth_relative,"
            "employee_3_months_growth_unique,"
            "employee_3_months_growth_percentile,"
            "employee_3_months_growth_relative,"
            "employee_3_months_growth_delta,"
            "employee_6_months_growth_unique,"
            "employee_6_months_growth_percentile,"
            "employee_6_months_growth_relative,"
            "employee_6_months_growth_delta,"
            "employee_12_months_growth_unique,"
            "employee_12_months_growth_percentile,"
            "employee_12_months_growth_relative,"
            "employee_12_months_growth_delta,"
            "innovation_corporate_rank,"
            "kpi_summary,"
            "team,investors,fundings,"
            "limited_partner_investments,known_limited_partners,"
            "job_openings,"
            "tech_stack_predictleads,"
            "sustainable_development_goals,"
            "data_type,"
            "pic_number,patents_count,"
            "dealroom_signal"
        )

    # ============================================================================
    # STREAMING BULK EXPORT — no checkpoints, no merge, retries 502-friendly
    # ============================================================================
    def bulk_export_simple(
        self,
        output_file: str = "dealroom_full.csv.gz",
        page_size: int = 100
    ):

        # Prepare file
        if os.path.exists(output_file):
            os.remove(output_file)

        next_page_id = None
        total = 0
        page = 0

        logger.info("🚀 Starting bulk export (streaming, no checkpoints)")
        logger.info(f"Output: {output_file}")

        while True:

            page += 1
            payload = {
                "fields": self.get_all_company_fields(),
                "limit": page_size
            }
            if next_page_id:
                payload["next_page_id"] = next_page_id

            # Retry loop (safe within 10 minutes)
            for attempt in range(6):
                try:
                    r = requests.post(
                        f"{self.base_url}/companies/bulk",
                        json=payload,
                        headers=self.headers,
                        timeout=60
                    )
                    if r.status_code in (502, 503, 504):
                        wait = 3 + attempt * 2
                        logger.warning(f"⚠️ {r.status_code} server error. Retry {attempt+1}/6 in {wait}s")
                        time.sleep(wait)
                        continue

                    r.raise_for_status()
                    break

                except Exception as e:
                    wait = 3 + attempt * 2
                    logger.error(f"Exception: {e}. Retry {attempt+1}/6 in {wait}s")
                    time.sleep(wait)
            else:
                logger.error("❌ Server kept failing. Stopping cleanly.")
                break

            data = r.json()
            items = data.get("items", [])
            next_page_id = data.get("next_page_id")

            if not items:
                logger.info("No more items returned. Ending.")
                break

            # Write batch to file
            df = pd.json_normalize(items, sep="__")
            df.to_csv(
                output_file,
                mode="a",
                header=not os.path.exists(output_file),
                index=False,
                compression="gzip"
            )

            total += len(df)

            print(f"\rPage {page} | Batch {len(df)} | Total {total:,}", end="", flush=True)

            # Safety sleep
            time.sleep(1)

            if not next_page_id:
                logger.info("Reached end of dataset.")
                break

        print("\n")
        logger.info(f"✅ Done. Total rows written: {total:,}")
        return output_file


# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    API_KEY = "f353c6bd59a492aaa4eac73f84aa7c9cf30cc7fd"

    importer = DealroomImporter(API_KEY)
    importer.bulk_export_simple(
        output_file="dealroom_bulk_stream.csv.gz",
        page_size=100
    )

import subprocess

OUTPUT_FILE = "dealroom_bulk_stream.csv.gz"
import subprocess

BUCKET_PATH = "gs://dealroom-dump-478609/dealroom_bulk_stream.csv.gz"

subprocess.run(["gsutil", "cp", OUTPUT_FILE, BUCKET_PATH], check=True)
print("Uploaded to", BUCKET_PATH)
