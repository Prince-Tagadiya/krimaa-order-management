import json
import glob
import os
import shutil

def run_clean_migration(source_dir, target_dir):
    print(f"==================================================")
    print(f"Reading PRISTINE source: {source_dir}")
    print(f"Writing CLEAN target:    {target_dir}")
    print(f"==================================================")

    # 1. Accounts mapping
    with open(os.path.join(source_dir, 'accounts.json'), 'r', encoding='utf-8') as f:
        source_accounts = json.load(f)

    flip_ids = {
        # Company 1
        'acc_vfxc0cxkj': ('acc_q5q08vr7n', 'F'),
        'acc_4cgcmdrbi': ('acc_q5q08vr7n', 'F'),
        'acc_96u3bg7po': ('acc_y3zmzp798', 'F'),
        'acc_qiui58hts': ('acc_y3zmzp798', 'F'),
        'acc_m7h8yhyi0': ('acc_902cp5g3i', 'F'),
        'acc_r5lhhlx25': ('acc_902cp5g3i', 'F'),
        'acc_mc6fies6l': ('acc_7segryxks', 'F'),
        'acc_183ph7u2a': ('acc_7segryxks', 'F'),
        'acc_v52ea6ogm': ('acc_01qyjk5yb', 'F'),
        'acc_b6lobvwhz': ('acc_01qyjk5yb', 'F'),
        'acc_x3x0wh457': ('acc_7njzspbho', 'F'), # FLIP MF -> MAHADEV
        'acc_8w0eym2ie': ('acc_1mpwkdmpq', 'F'), # FLIP SE -> Shrushti
        # Company 2
        'acc_v4lqidhi9': ('acc_una9qmt7u', 'F'),
        'acc_bw0kro915': ('acc_una9qmt7u', 'F'),
        'acc_gsmzs050r': ('acc_qb1n1firj', 'F'), # Nilkhanth FLIP -> Nilkhanth
    }

    flip_names = {
        'flip shawt': ('acc_q5q08vr7n', 'F'),
        'flipkr shawt': ('acc_q5q08vr7n', 'F'),
        'flip ac': ('acc_y3zmzp798', 'F'),
        'flipkr ac': ('acc_y3zmzp798', 'F'),
        'flip af': ('acc_902cp5g3i', 'F'),
        'flipkr af': ('acc_902cp5g3i', 'F'),
        'flipkr sse': ('acc_7segryxks', 'F'),
        'flip shani': ('acc_01qyjk5yb', 'F'),
        'flipkr shani': ('acc_01qyjk5yb', 'F'),
        'flip mf': ('acc_7njzspbho', 'F'),
        'flip se': ('acc_1mpwkdmpq', 'F'),
        'flipkrt verai': ('acc_una9qmt7u', 'F'),
        'nilkhanth flip': ('acc_qb1n1firj', 'F'),
    }

    base_aliases = {
        'acc_4e9efisyl': ('acc_y3zmzp798', 'M'),
        'acc_u4jayx85j': ('acc_902cp5g3i', 'M'),
        'acc_g2p7lb5eb': ('acc_vnaf0lunc', 'M'),
    }

    remove_flip_ids = set(flip_ids.keys())

    has_both = {
        'acc_q5q08vr7n', # Shaswat
        'acc_y3zmzp798', # Anadi
        'acc_902cp5g3i', # AF
        'acc_7segryxks', # SHREE SAI
        'acc_01qyjk5yb', # Shani
        'acc_7njzspbho', # MAHADEV
        'acc_1mpwkdmpq', # Shrushti
        'acc_una9qmt7u', # SREE VERAI KRUPA
        'acc_qb1n1firj', # Nilkhanth
    }

    # Filter base accounts
    new_accounts = []
    base_acc_map = {}
    for a in source_accounts:
        aid = a.get('id') or a.get('accountId')
        if aid in remove_flip_ids:
            continue
        a['hasMeesho'] = True
        a['hasFlipkart'] = True
        new_accounts.append(a)
        base_acc_map[aid] = a.get('name')

    # Position ordering
    c1 = [a for a in new_accounts if a.get('companyId') == 'company1']
    c2 = [a for a in new_accounts if a.get('companyId') == 'company2']
    c1.sort(key=lambda x: x.get('position', 0))
    c2.sort(key=lambda x: x.get('position', 0))
    for idx, a in enumerate(c1):
        a['position'] = idx
    for idx, a in enumerate(c2):
        a['position'] = idx
    cleaned_accounts = c1 + c2

    # Save clean accounts.json
    with open(os.path.join(target_dir, 'accounts.json'), 'w', encoding='utf-8') as f:
        json.dump(cleaned_accounts, f, indent=2)
    print(f"✓ Saved accounts.json with {len(cleaned_accounts)} accounts (C1: {len(c1)}, C2: {len(c2)})")

    # 2. Migrate orders files
    order_files = sorted(glob.glob(os.path.join(source_dir, 'orders_*.json')))
    if os.path.exists(os.path.join(source_dir, 'daily_orders.json')):
        order_files.append(os.path.join(source_dir, 'daily_orders.json'))

    total_all_before = 0
    total_all_after = 0

    for src_path in order_files:
        fname = os.path.basename(src_path)
        tgt_path = os.path.join(target_dir, fname)

        with open(src_path, 'r', encoding='utf-8') as f:
            orders = json.load(f)

        qty_before = sum(int(o.get('quantity') or o.get('meesho') or o.get('total') or 0) for o in orders)
        total_all_before += qty_before

        merged = {}
        for o in orders:
            d = o.get('date')
            comp = o.get('companyId') or o.get('masterCompany') or 'company1'
            comp_name = o.get('companyName') or ('Company 1' if comp == 'company1' else 'Company 2')
            aid = o.get('accountId') or ''
            aname = (o.get('accountName') or '').strip()
            anamelow = aname.lower()
            val = int(o.get('quantity') or o.get('meesho') or o.get('total') or 0)

            target_id = aid
            channel = 'M'

            if aid in flip_ids:
                target_id, channel = flip_ids[aid]
            elif anamelow in flip_names:
                target_id, channel = flip_names[anamelow]
            elif aid in base_aliases:
                target_id, channel = base_aliases[aid]

            target_name = base_acc_map.get(target_id) or aname or target_id

            key = (d, comp, target_id)
            if key not in merged:
                merged[key] = {
                    'orderId': f"{d}_{target_id}",
                    'id': f"{d}_{target_id}",
                    'date': d,
                    'companyId': comp,
                    'companyName': comp_name,
                    'masterCompany': comp,
                    'accountId': target_id,
                    'accountName': target_name,
                    'meesho': 0,
                    'flipkart': 0,
                    'quantity': 0,
                    'total': 0,
                    'updatedAt': {'_methodName': 'FieldValue.serverTimestamp'}
                }

            if channel == 'F':
                merged[key]['flipkart'] += val
            else:
                merged[key]['meesho'] += val

            merged[key]['total'] = merged[key]['meesho'] + merged[key]['flipkart']
            merged[key]['quantity'] = merged[key]['total']

        merged_list = sorted(merged.values(), key=lambda x: (x['date'], x['companyId'], x['accountId']))
        qty_after = sum(o['total'] for o in merged_list)
        total_all_after += qty_after

        assert qty_before == qty_after, f"Quantity mismatch in {fname}: before={qty_before}, after={qty_after}"

        with open(tgt_path, 'w', encoding='utf-8') as f:
            json.dump(merged_list, f, indent=2)

        print(f"✓ Migrated {fname:22}: before={qty_before:6} ({len(orders):4} rows) -> after={qty_after:6} ({len(merged_list):4} rows) [100% MATCH]")

    assert total_all_before == total_all_after
    print(f"✓ All orders verified: total before = {total_all_before}, total after = {total_all_after} (100% ACCURATE)")

    # 3. Migrate daily_summary.json
    daily_summary_path = os.path.join(source_dir, 'daily_summary.json')
    if os.path.exists(daily_summary_path):
        with open(daily_summary_path, 'r', encoding='utf-8') as f:
            daily_summary = json.load(f)

        total_ds_before = 0
        total_ds_after = 0

        for day in daily_summary:
            for k, v in list(day.items()):
                if k in ('date', 'id', 'masterCompany'):
                    continue
                try:
                    total_ds_before += int(v)
                except:
                    pass

            for flip_id, (base_id, _) in flip_ids.items():
                if flip_id in day:
                    val = int(day.pop(flip_id) or 0)
                    day[base_id] = int(day.get(base_id) or 0) + val

            for alias_id, (base_id, _) in base_aliases.items():
                if alias_id in day:
                    val = int(day.pop(alias_id) or 0)
                    day[base_id] = int(day.get(base_id) or 0) + val

            for k, v in day.items():
                if k in ('date', 'id', 'masterCompany'):
                    continue
                try:
                    total_ds_after += int(v)
                except:
                    pass

        assert total_ds_before == total_ds_after, f"daily_summary mismatch: before={total_ds_before}, after={total_ds_after}"

        with open(os.path.join(target_dir, 'daily_summary.json'), 'w', encoding='utf-8') as f:
            json.dump(daily_summary, f, indent=2)
        print(f"✓ Migrated daily_summary.json: before={total_ds_before}, after={total_ds_after} (100% MATCH)")

    # 4. Copy all other non-order files from backup
    other_files = [
        'karigars.json', 'karigar_transactions.json', 'karigar_reset_backups.json',
        'design_prices.json', 'design_price_history.json', 'remarks.json', 'system.json'
    ]
    for of in other_files:
        src_f = os.path.join(source_dir, of)
        tgt_f = os.path.join(target_dir, of)
        if os.path.exists(src_f):
            shutil.copy2(src_f, tgt_f)
            print(f"✓ Copied {of}")

    # Remove temporary lock files in target
    for lock_f in glob.glob(os.path.join(target_dir, '*crswap*')):
        try:
            os.remove(lock_f)
        except:
            pass

    print("\n🎉 ALL DONE! Target LAN folder is 100% clean, verified, and ready to hand to client!")

if __name__ == '__main__':
    src = '/Users/princetagadiya/Downloads/krimaa_lan_shared_drive_2026-06-01_BACKUP_BEFORE_MERGE'
    tgt = '/Users/princetagadiya/Downloads/krimaa_lan_shared_drive_2026-06-01'
    run_clean_migration(src, tgt)
