import json
import glob
import os
import shutil

MASTER_DIR = '/Users/princetagadiya/Downloads/krimaa_merged_FINAL'
DIR_KRIMAA = '/Users/princetagadiya/Downloads/krimaa_FINAL_CLEAN'
DIR_DHYAN = '/Users/princetagadiya/Downloads/dhyan_FINAL_CLEAN'

def create_isolated_backup(company_id, target_dir, label):
    print("=" * 60)
    print(f"GENERATING ISOLATED BACKUP FOR: {label} ({company_id})")
    print(f"TARGET DIRECTORY: {target_dir}")
    print("=" * 60)

    os.makedirs(target_dir, exist_ok=True)

    # 1. ACCOUNTS
    with open(os.path.join(MASTER_DIR, 'accounts.json'), 'r', encoding='utf-8') as f:
        all_accounts = json.load(f)

    comp_accounts = [dict(a) for a in all_accounts if a.get('companyId') == company_id]
    comp_accounts.sort(key=lambda x: int(x.get('position', 999)))
    for idx, a in enumerate(comp_accounts):
        a['position'] = idx

    with open(os.path.join(target_dir, 'accounts.json'), 'w', encoding='utf-8') as f:
        json.dump(comp_accounts, f, indent=2)
    print(f"✓ Saved {len(comp_accounts)} accounts to accounts.json")
    for a in comp_accounts:
        m = 'M' if a.get('hasMeesho') else '-'
        fl = 'F' if a.get('hasFlipkart') else '-'
        print(f"   {a['position']:2d}: {a['name']:20s} ({m}/{fl}) id={a['id']}")

    comp_acc_ids = set(a['id'] for a in comp_accounts)

    # 2. MONTHLY ORDERS
    month_files = sorted(glob.glob(os.path.join(MASTER_DIR, 'orders_*.json')))
    tot_orders = 0
    daily_summary_map = {}

    for mf in month_files:
        fname = os.path.basename(mf)
        orders = json.load(open(mf, 'r', encoding='utf-8'))
        filtered = [o for o in orders if o.get('companyId') == company_id or o.get('accountId') in comp_acc_ids]
        filtered.sort(key=lambda x: (x['date'], x['accountId']))

        qty = sum(o.get('total', 0) for o in filtered)
        tot_orders += qty

        for o in filtered:
            d = o['date']
            aid = o['accountId']
            tot = o.get('total', 0)
            if d not in daily_summary_map:
                daily_summary_map[d] = {}
            daily_summary_map[d][aid] = daily_summary_map[d].get(aid, 0) + tot

        with open(os.path.join(target_dir, fname), 'w', encoding='utf-8') as f:
            json.dump(filtered, f, indent=2)
        print(f"✓ {fname:20s}: {qty:6d} orders ({len(filtered):4d} rows)")

    print(f"✓ Total monthly orders for {label}: {tot_orders}")

    # 3. DAILY ORDERS
    daily_orders_master = json.load(open(os.path.join(MASTER_DIR, 'daily_orders.json'), 'r', encoding='utf-8'))
    daily_filtered = [o for o in daily_orders_master if o.get('companyId') == company_id or o.get('accountId') in comp_acc_ids]
    daily_filtered.sort(key=lambda x: (x['date'], x['accountId']))

    with open(os.path.join(target_dir, 'daily_orders.json'), 'w', encoding='utf-8') as f:
        json.dump(daily_filtered, f, indent=2)
    daily_qty = sum(o.get('total', 0) for o in daily_filtered)
    print(f"✓ daily_orders.json   : {daily_qty:6d} orders ({len(daily_filtered):4d} rows)")

    # 4. DAILY SUMMARY
    daily_summary_list = []
    for dt in sorted(daily_summary_map.keys()):
        day_obj = {
            'id': dt,
            'date': dt,
            'masterCompany': company_id
        }
        for aid, tot in sorted(daily_summary_map[dt].items()):
            day_obj[aid] = tot
        daily_summary_list.append(day_obj)

    with open(os.path.join(target_dir, 'daily_summary.json'), 'w', encoding='utf-8') as f:
        json.dump(daily_summary_list, f, indent=2)
    print(f"✓ Generated daily_summary.json with {len(daily_summary_list)} days")

    # 5. REMARKS
    remarks_master = json.load(open(os.path.join(MASTER_DIR, 'remarks.json'), 'r', encoding='utf-8'))
    with open(os.path.join(target_dir, 'remarks.json'), 'w', encoding='utf-8') as f:
        json.dump(remarks_master, f, indent=2)
    print("✓ Copied remarks.json")

    # 6. KARIGARS & OTHER META
    meta_files = [
        'karigars.json', 'karigar_transactions.json', 'karigar_reset_backups.json',
        'design_prices.json', 'design_price_history.json', 'system.json'
    ]
    for mf in meta_files:
        src = os.path.join(MASTER_DIR, mf)
        tgt = os.path.join(target_dir, mf)
        if os.path.exists(src):
            shutil.copy2(src, tgt)
            print(f"✓ Copied {mf}")

    # Empty sync queue
    with open(os.path.join(target_dir, 'sync_queue.json'), 'w', encoding='utf-8') as f:
        json.dump([], f)
    print("✓ Initialized sync_queue.json")

    # Clean lock files
    for lk in glob.glob(os.path.join(target_dir, '*crswap*')) + glob.glob(os.path.join(target_dir, '*lock*')):
        try: os.remove(lk)
        except: pass

    print(f"\n🎉 {label} backup is 100% clean and ready at: {target_dir}\n")

if __name__ == '__main__':
    create_isolated_backup('company1', DIR_KRIMAA, 'KRIMAA (Shree Sai)')
    create_isolated_backup('company2', DIR_DHYAN, 'DHYAN')
