import json
import glob
import os
import shutil

SRC_KRIMAA = '/Users/princetagadiya/Downloads/krimaa_client_FINAL'
SRC_DHYAN = '/Users/princetagadiya/Downloads/dhyan'
TARGET_DIR = '/Users/princetagadiya/Downloads/krimaa_merged_FINAL'

def run_merge():
    print("=" * 60)
    print(f"SOURCE KRIMAA: {SRC_KRIMAA}")
    print(f"SOURCE DHYAN:  {SRC_DHYAN}")
    print(f"TARGET OUTPUT: {TARGET_DIR}")
    print("=" * 60)

    os.makedirs(TARGET_DIR, exist_ok=True)

    # 1. ACCOUNTS MAPPINGS
    flip_ids = {
        # Company 1
        'acc_vfxc0cxkj': ('acc_q5q08vr7n', 'F'), # Flipkr Shawt -> Shaswat
        'acc_4cgcmdrbi': ('acc_q5q08vr7n', 'F'), # Flipkr Shawt (legacy) -> Shaswat
        'acc_96u3bg7po': ('acc_y3zmzp798', 'F'), # Flipkr AC -> Anadi
        'acc_qiui58hts': ('acc_y3zmzp798', 'F'), # Flipkr AC (legacy) -> Anadi
        'acc_m7h8yhyi0': ('acc_902cp5g3i', 'F'), # Flipkr AF -> AF
        'acc_r5lhhlx25': ('acc_902cp5g3i', 'F'), # Flipkr AF (legacy) -> AF
        'acc_mc6fies6l': ('acc_7segryxks', 'F'), # Flipkr SSE -> SHREE SAI
        'acc_183ph7u2a': ('acc_7segryxks', 'F'), # Flipkr SSE (legacy) -> SHREE SAI
        'acc_v52ea6ogm': ('acc_01qyjk5yb', 'F'), # Flipkr Shani -> Shani
        'acc_b6lobvwhz': ('acc_01qyjk5yb', 'F'), # Flipkr Shani (legacy) -> Shani
        'acc_x3x0wh457': ('acc_7njzspbho', 'F'), # FLIP MF / FLIPK MF -> MAHADEV
        'acc_8w0eym2ie': ('acc_1mpwkdmpq', 'F'), # FLIP SE / FLIPK SE -> Shrushti
        # Company 2
        'acc_v4lqidhi9': ('acc_una9qmt7u', 'F'), # FLIPKRT VERAI -> SREE VERAI KRUPA
        'acc_bw0kro915': ('acc_una9qmt7u', 'F'), # FLIPKRT VERAI (legacy) -> SREE VERAI KRUPA
        'acc_gsmzs050r': ('acc_qb1n1firj', 'F'), # FLIPKRT Nilkhanth -> Nilkhanth
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
        'flipk mf': ('acc_7njzspbho', 'F'),
        'flip se': ('acc_1mpwkdmpq', 'F'),
        'flipk se': ('acc_1mpwkdmpq', 'F'),
        'flipkrt verai': ('acc_una9qmt7u', 'F'),
        'flipkrt nilkhanth': ('acc_qb1n1firj', 'F'),
        'nilkhanth flip': ('acc_qb1n1firj', 'F'),
    }

    base_aliases = {
        'acc_4e9efisyl': ('acc_y3zmzp798', 'M'), # Anadi (legacy) -> Anadi
        'acc_u4jayx85j': ('acc_902cp5g3i', 'M'), # AF / Akshar (legacy) -> AF
        'acc_g2p7lb5eb': ('acc_vnaf0lunc', 'M'), # Ramchandi (legacy) -> Ramch
    }

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

    c2_account_ids = {
        'acc_una9qmt7u', 'acc_v4lqidhi9', 'acc_qb1n1firj', 'acc_gsmzs050r',
        'acc_b6a6t1033', 'acc_0odzl12o8', 'acc_010743gi9', 'acc_dz0vr4i4e',
        'acc_bagch269w', 'acc_bw0kro915'
    }

    # Load source accounts
    source_accounts = {}
    for src in [SRC_KRIMAA, SRC_DHYAN]:
        fpath = os.path.join(src, 'accounts.json')
        if os.path.exists(fpath):
            with open(fpath, 'r', encoding='utf-8') as f:
                for a in json.load(f):
                    aid = a.get('id') or a.get('accountId')
                    if aid not in flip_ids:
                        source_accounts[aid] = a

    # Clean base accounts list
    base_acc_map = {}
    cleaned_c1 = []
    cleaned_c2 = []
    for aid, a in source_accounts.items():
        base_acc_map[aid] = a.get('name')
        a_copy = dict(a)
        a_copy['id'] = aid
        a_copy['accountId'] = aid
        a_copy['hasMeesho'] = True
        a_copy['hasFlipkart'] = (aid in has_both)
        if a_copy.get('companyId') == 'company2':
            cleaned_c2.append(a_copy)
        else:
            cleaned_c1.append(a_copy)

    cleaned_c1.sort(key=lambda x: int(x.get('position', 999)))
    cleaned_c2.sort(key=lambda x: int(x.get('position', 999)))

    for idx, a in enumerate(cleaned_c1):
        a['position'] = idx
    for idx, a in enumerate(cleaned_c2):
        a['position'] = idx

    all_cleaned_accounts = cleaned_c1 + cleaned_c2
    with open(os.path.join(TARGET_DIR, 'accounts.json'), 'w', encoding='utf-8') as f:
        json.dump(all_cleaned_accounts, f, indent=2)

    print(f"✓ Saved accounts.json with {len(all_cleaned_accounts)} accounts (C1: {len(cleaned_c1)}, C2: {len(cleaned_c2)})")
    for a in all_cleaned_accounts:
        m_flag = 'M' if a.get('hasMeesho') else '-'
        f_flag = 'F' if a.get('hasFlipkart') else '-'
        print(f"   [{a['companyId']}] {a['position']:2d}: {a['name']:20s} ({m_flag}/{f_flag}) id={a['id']}")

    # 2. ORDERS MIGRATION
    all_order_files = set()
    for src in [SRC_KRIMAA, SRC_DHYAN]:
        for f in glob.glob(os.path.join(src, 'orders_*.json')) + [os.path.join(src, 'daily_orders.json')]:
            if os.path.exists(f):
                all_order_files.add(os.path.basename(f))

    grand_before = 0
    grand_after = 0
    daily_summary_map = {} # date -> {aid -> total}

    for fname in sorted(all_order_files):
        f_krimaa = os.path.join(SRC_KRIMAA, fname)
        f_dhyan = os.path.join(SRC_DHYAN, fname)

        k_orders = json.load(open(f_krimaa, 'r', encoding='utf-8')) if os.path.exists(f_krimaa) else []
        d_orders = json.load(open(f_dhyan, 'r', encoding='utf-8')) if os.path.exists(f_dhyan) else []

        def get_raw_key(o):
            aid = o.get('accountId') or ''
            cid = o.get('companyId') or o.get('masterCompany')
            if not cid:
                cid = 'company2' if aid in c2_account_ids else 'company1'
            return (o.get('date'), cid, aid)

        # Merge raw: Company 1 from krimaa (authoritative), Company 2 from dhyan (authoritative)
        raw_indexed = {}
        for o in k_orders:
            key = get_raw_key(o)
            raw_indexed[key] = o

        for o in d_orders:
            key = get_raw_key(o)
            dt, cid, aid = key
            if cid == 'company2' or key not in raw_indexed:
                raw_indexed[key] = o

        # Calculate before total for this file
        qty_before = sum(int(o.get('quantity') or o.get('meesho') or o.get('total') or 0) for o in raw_indexed.values())
        grand_before += qty_before

        merged_orders = {}
        for key, o in raw_indexed.items():
            dt, comp, aid = key
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

            m_val = 0
            f_val = 0
            if channel == 'F':
                f_val = val
            else:
                if o.get('flipkart') is not None and o.get('meesho') is not None:
                    m_val = int(o.get('meesho') or 0)
                    f_val = int(o.get('flipkart') or 0)
                else:
                    m_val = val

            fkey = (dt, comp, target_id)
            if fkey not in merged_orders:
                target_name = base_acc_map.get(target_id) or aname or target_id
                comp_name = 'Shree Sai' if comp == 'company1' else 'Dhyan'
                merged_orders[fkey] = {
                    'orderId': f"{dt}_{target_id}",
                    'id': f"{dt}_{target_id}",
                    'date': dt,
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
            merged_orders[fkey]['meesho'] += m_val
            merged_orders[fkey]['flipkart'] += f_val
            merged_orders[fkey]['total'] = merged_orders[fkey]['meesho'] + merged_orders[fkey]['flipkart']
            merged_orders[fkey]['quantity'] = merged_orders[fkey]['total']

            # Accumulate into daily summary (exclude daily_orders.json from double counting)
            if fname != 'daily_orders.json':
                if dt not in daily_summary_map:
                    daily_summary_map[dt] = {}
                daily_summary_map[dt][target_id] = daily_summary_map[dt].get(target_id, 0) + (m_val + f_val)

        # Sort order rows
        final_list = sorted(merged_orders.values(), key=lambda x: (x['date'], x['companyId'], x['accountId']))
        qty_after = sum(x['total'] for x in final_list)
        grand_after += qty_after

        assert qty_before == qty_after, f"Mismatch in {fname}: before={qty_before}, after={qty_after}"

        with open(os.path.join(TARGET_DIR, fname), 'w', encoding='utf-8') as f:
            json.dump(final_list, f, indent=2)

        print(f"✓ {fname:20s}: before={qty_before:6d} -> after={qty_after:6d} rows={len(final_list):4d} [100% MATCH]")

    assert grand_before == grand_after
    print(f"✓ All order files verified: grand_before = {grand_before}, grand_after = {grand_after} (100% ACCURATE)")

    # 3. BUILD DAILY SUMMARY
    daily_summary_list = []
    for dt in sorted(daily_summary_map.keys()):
        day_obj = {
            'id': dt,
            'date': dt,
            'masterCompany': 'multi_company'
        }
        for aid, tot in sorted(daily_summary_map[dt].items()):
            day_obj[aid] = tot
        daily_summary_list.append(day_obj)

    with open(os.path.join(TARGET_DIR, 'daily_summary.json'), 'w', encoding='utf-8') as f:
        json.dump(daily_summary_list, f, indent=2)
    print(f"✓ Generated daily_summary.json with {len(daily_summary_list)} days")

    # 4. REMARKS (use Krimaa which has all 13 remarks)
    shutil.copy2(os.path.join(SRC_KRIMAA, 'remarks.json'), os.path.join(TARGET_DIR, 'remarks.json'))
    print("✓ Copied remarks.json")

    # 5. KARIGARS & OTHER META
    meta_files = [
        'karigars.json', 'karigar_transactions.json', 'karigar_reset_backups.json',
        'design_prices.json', 'design_price_history.json', 'system.json'
    ]
    for mf in meta_files:
        src_path = os.path.join(SRC_KRIMAA, mf)
        tgt_path = os.path.join(TARGET_DIR, mf)
        if os.path.exists(src_path):
            shutil.copy2(src_path, tgt_path)
            print(f"✓ Copied {mf}")

    # Empty sync queue
    with open(os.path.join(TARGET_DIR, 'sync_queue.json'), 'w', encoding='utf-8') as f:
        json.dump([], f)
    print("✓ Initialized sync_queue.json")

    # Remove any temp lock files
    for lk in glob.glob(os.path.join(TARGET_DIR, '*crswap*')) + glob.glob(os.path.join(TARGET_DIR, '*lock*')):
        try: os.remove(lk)
        except: pass

    print("=" * 60)
    print("🎉 SUCCESS! Clean merged database is ready at:")
    print(f"   {TARGET_DIR}")
    print("=" * 60)

if __name__ == '__main__':
    run_merge()
