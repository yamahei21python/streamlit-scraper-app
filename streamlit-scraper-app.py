import streamlit as st
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin, urlparse
import re
import time
from datetime import datetime, timedelta
import concurrent.futures
import json
import io

# --- データ定義 (変更なし) ---
PREFECTURES = {'東京': 'tokyo', '大阪': 'osaka', '香川': 'kagawa', '北海道': 'hokkaido', '青森': 'aomori', '岩手': 'iwate', '宮城': 'miyagi', '秋田': 'akita', '山形': 'yamagata', '福島': 'fukushima', '茨城': 'ibaraki', '栃木': 'tochigi', '群馬': 'gunma', '埼玉': 'saitama', '千葉': 'chiba', '神奈川': 'kanagawa', '新潟': 'niigata', '富山': 'toyama', '石川': 'ishikawa', '福井': 'fui', '山梨': 'yamanashi', '長野': 'nagano', '岐阜': 'gifu', '静岡': 'shizuoka', '愛知': 'aichi', '三重': 'mie', '滋賀': 'shiga', '京都': 'kyoto', '兵庫': 'hyogo', '奈良': 'nara', '和歌山': 'wakayama', '鳥取': 'tottori', '島根': 'shimane', '岡山': 'okayama', '広島': 'hiroshima', '山口': 'yamaguchi', '徳島': 'tokushima', '愛媛': 'ehime', '高知': 'kochi', '福岡': 'fukuoka', '佐賀': 'saga', '長崎': 'nagasaki', '熊本': 'kumamoto', '大分': 'oita', '宮崎': 'miyazaki', '鹿児島': 'kagoshima', '沖縄': 'okinawa'}
AGE_MAP = {'18～19歳': 'typ101', '20～24歳': 'typ102', '25～29歳': 'typ103'}
HEIGHT_MAP = {'149cm以下': 'typ201', '150～154cm': 'typ202', '155～159cm': 'typ203', '160～164cm': 'typ204'}
BUST_MAP = {'Aカップ': 'typ301', 'Bカップ': 'typ302', 'Cカップ': 'typ303', 'Dカップ': 'typ304', 'Eカップ': 'typ305', 'Fカップ': 'typ306', 'Gカップ': 'typ307', 'Hカップ': 'typ308', 'Iカップ以上': 'typ309'}
FEATURE_MAP = {'ニューフェイス': 'typ601', 'お店NO.1・2・3': 'typ602'}
ALL_TYPS = {**AGE_MAP, **HEIGHT_MAP, **BUST_MAP, **FEATURE_MAP}


# --- ヘルパー関数群 (変更なし) ---
def create_gallery_html(image_urls):
    if not isinstance(image_urls, list) or not image_urls: return ''
    image_paths_json = json.dumps(image_urls)
    return f'<div class="gallery-container" data-images=\'{image_paths_json}\' data-current-index="0" onclick="nextImage(this)" style="cursor: pointer;"><img src="{image_urls[0]}" width="120" onerror="this.style.display=\'none\'"></div>'

def create_star_rating_html(sort_number):
    try: num = int(sort_number)
    except (ValueError, TypeError): num = 999
    if num == 21: stars = "★★★"
    elif num == 22: stars = "★★☆"
    elif num == 23: stars = "★☆☆"
    else: stars = "☆☆☆"; num = 999
    sort_key_html = f'<span style="display: none;">{num:03d}</span>'
    star_display_html = f'<span style="font-size: 1.1em; color: #f2b01e;">{stars}</span>'
    return f'{sort_key_html}{star_display_html}'

def parse_style(style_text):
    data = {"身長(cm)": None, "バスト(cm)": None, "カップ": None, "ウェスト(cm)": None, "ヒップ(cm)": None}
    if style_text and (match := re.search(r'T(\d+).*?･(\d+)\((.*?)\)･(\d+)･(\d+)', style_text)):
        try: data.update({"身長(cm)": int(match.group(1)), "バスト(cm)": int(match.group(2)), "カップ": match.group(3), "ウェスト(cm)": int(match.group(4)), "ヒップ(cm)": int(match.group(5))})
        except (ValueError, IndexError): pass
    return data

def parse_age(age_text):
    if age_text and (match := re.search(r'\d+', age_text)): return int(match.group(0))
    return None

def parse_sortable_time(text):
    if text and (match := re.search(r'(\d{1,2}:\d{2})', text)): return match.group(1)
    return None

def get_girl_details(profile_url, session, headers):
    details = {"本日の出勤予定": None, "次回出勤": None, "ギャラリーURL": [], "WEB人気の星": 999, "週合計出勤日数": 0, "週合計勤務時間": 0.0, "口コミ数": 0}
    if not profile_url: return details
    base_site_url = "https://www.cityheaven.net/"
    if (girlid_match := re.search(r'girlid-(\d+)', profile_url)) and (shop_url_match := re.search(r'(https?://.*?/girlid-)', profile_url)):
        girl_id, shop_base_url = girlid_match.group(1), shop_url_match.group(1).replace('girlid-', '')
        review_url = f"{shop_base_url}reviews/?girlid={girl_id}"
        try:
            review_res = session.get(review_url, timeout=10, headers=headers)
            if review_res.status_code == 200:
                review_soup = BeautifulSoup(review_res.content, 'html.parser')
                if (total_div := review_soup.find('div', class_='review-total')) and (count_match := re.search(r'(\d+)件', total_div.get_text())):
                    details["口コミ数"] = int(count_match.group(1))
        except requests.exceptions.RequestException: pass
    try:
        res = session.get(profile_url, timeout=30, headers=headers)
        if res.status_code != 200: return details
        soup = BeautifulSoup(res.content, 'html.parser')
        total_work_hours, total_work_days = 0.0, 0
        if schedule_list := soup.find('ul', id='girl_sukkin'):
            today_str = datetime.now().strftime("%-m/%-d")
            for item in schedule_list.find_all('li'):
                if (date_tag := item.find('dt')) and today_str in date_tag.get_text():
                    if dd_tag := item.find('dd'): details["本日の出勤予定"] = dd_tag.get_text(separator='-', strip=True)
                if dd_tag := item.find('dd'):
                    time_text = dd_tag.get_text(strip=True)
                    if match := re.search(r'(\d{1,2}:\d{1,2}).*?(\d{1,2}:\d{1,2})', time_text):
                        total_work_days += 1
                        try:
                            start_time, end_time = datetime.strptime(match.group(1), '%H:%M'), datetime.strptime(match.group(2), '%H:%M')
                            if end_time < start_time: end_time += timedelta(days=1)
                            total_work_hours += (end_time - start_time).total_seconds() / 3600
                        except ValueError: continue
        details["週合計出勤日数"], details["週合計勤務時間"] = total_work_days, total_work_hours
        if next_time_tag := soup.find('td', class_='shukkin-sugunavitext'):
            details["次回出勤"] = parse_sortable_time(next_time_tag.get_text(strip=True))
        if star_img_tag := soup.find('img', class_='yoyaku_girlmark'):
            if match := re.search(r'yoyaku_(\d+)\.png', star_img_tag.get('src', '')):
                details["WEB人気の星"] = int(match.group(1))
        image_urls = []
        if photo_container := soup.find('div', class_='profile_photo'):
            for img_tag in photo_container.find_all('img'):
                if src := img_tag.get('src') or img_tag.get('data-src'):
                    if 'grpb' in src: image_urls.append(urljoin(base_site_url, src))
        if diary := soup.find('div', id='girlprofile_diary'):
            for item in diary.find_all('div', class_='thm'):
                if (img_tag := item.find('img')) and (src := img_tag.get('src')) and 'grdr' in src:
                    image_urls.append(urljoin(base_site_url, src))
        details["ギャラリーURL"] = list(dict.fromkeys(image_urls))[:6]
    except requests.exceptions.RequestException: pass
    return details

def run_scraper(params, progress_bar, status_text):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
    base_url = "https://www.cityheaven.net/"
    prefecture_path, filter_path, debug_mode, hide_inactive, page_limit = params
    target_path = f"{prefecture_path}/{filter_path}"
    all_girls_data = []
    with requests.Session() as session:
        retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter); session.mount("http://", adapter)
        try:
            status_text.text("総ページ数を確認中...")
            first_page_url = urljoin(base_url, target_path)
            response = session.get(first_page_url, timeout=30, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            last_page = 1
            if pagination_div := soup.find('div', class_='shop_nav_list'):
                for link in pagination_div.find_all('a'):
                    if link.text.isdigit() and (page_num := int(link.text)) > last_page: last_page = page_num
            pages_to_scrape = 1 if debug_mode else last_page
            if page_limit: pages_to_scrape = min(pages_to_scrape, page_limit)
            initial_girl_list = []
            for page in range(1, pages_to_scrape + 1):
                status_text.text(f"ページ {page}/{pages_to_scrape} の基本情報を取得中...")
                progress_bar.progress(page / pages_to_scrape * 0.2)
                if page > 1:
                    response = session.get(urljoin(base_url, f"{target_path}{page}/"), timeout=30, headers=headers)
                    soup = BeautifulSoup(response.content, 'html.parser')
                for item in soup.find_all('li', class_='girls-list'):
                    if not item.find('div', class_='maingirl'): continue
                    status = (s.text.strip() if (s := item.find('span', 'soku')) else None) or "-"
                    profile_link = urljoin(base_url, p.get('href')) if (p := item.find('a', 'shopimg')) else None
                    data = {"名前": n.text.strip() if (n := item.find('p', 'girlname').find('a')) else "NoName", "年齢": parse_age(a.text.strip() if (a := item.find('p', 'girlname').find('span')) else ''), "出勤状況": status, "次回出勤": parse_sortable_time(status), **parse_style(s.text.strip() if (s := item.find('p', 'girlstyle')) else ''), "プロフィールリンク": profile_link, "店舗名": (s.text.strip() if (s := item.find('p', 'shopname').find('a')) else None)}
                    initial_girl_list.append(data)
                time.sleep(0.5)
            status_text.text(f"全{len(initial_girl_list)}人の詳細情報を並列取得中...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_data = {executor.submit(get_girl_details, data['プロフィールリンク'], session, headers): data for data in initial_girl_list}
                progress_count = 0
                for future in concurrent.futures.as_completed(future_to_data):
                    original_data = future_to_data[future]
                    try:
                        details = future.result()
                        original_data.update(details)
                        if original_data["次回出勤"] is None: original_data["次回出勤"] = details.get("次回出勤")
                        all_girls_data.append(original_data)
                    except Exception: pass
                    progress_count += 1
                    progress_bar.progress(0.2 + (progress_count / len(initial_girl_list) * 0.8))
            if not all_girls_data:
                status_text.text("データが取得できませんでした。"); return pd.DataFrame()
            df = pd.DataFrame(all_girls_data)
            if hide_inactive: df = df[df['次回出勤'].notna()].copy()
            status_text.text("処理完了！"); return df
        except Exception as e:
            st.error(f"エラーが発生しました: {e}"); return pd.DataFrame()

# ★★★ f-stringの問題を修正したHTML生成関数 ★★★
def generate_html_report(df, title_text):
    if df.empty: return "<h1>データがありません</h1>"
    df_display = df.copy()

    # (...中略... データ変換のコードは変更なし)
    df_display['チェック'] = '<input type="checkbox" class="row-checkbox" style="cursor:pointer;">'
    df_display['名前'] = df_display.apply(lambda row: f'<a href="{row["プロフィールリンク"]}" target="_blank">{row["名前"]}</a>' if pd.notna(row['プロフィールリンク']) else row['名前'], axis=1)
    df_display['ギャラリー'] = df_display['ギャラリーURL'].apply(create_gallery_html)
    df_display['WEB人気'] = df_display['WEB人気の星'].apply(create_star_rating_html)
    num_cols = ['年齢', '身長(cm)', 'バスト(cm)', 'ウェスト(cm)', 'ヒップ(cm)', '口コミ数', '週合計出勤日数', '週合計勤務時間']
    for col in num_cols:
        if col in df_display.columns:
            df_display[col] = df_display[col].apply(lambda x: f"{x:.0f}" if pd.notna(x) and x > 0 else ("{:.1f}".format(x) if pd.notna(x) and x > 0 else ""))
    df_display.fillna("", inplace=True)
    rename_map = {"身長(cm)": "身長", "バスト(cm)": "バスト", "ウェスト(cm)": "ウェスト", "ヒップ(cm)": "ヒップ", "週合計出勤日数": "出勤日", "週合計勤務時間": "勤務時間", "店舗名": "店舗"}
    df_display.rename(columns=rename_map, inplace=True)
    desired_order = ["チェック", "名前", "ギャラリー", "年齢", "身長", "カップ", "WEB人気", "口コミ数", "出勤日", "勤務時間", "出勤状況", "本日の出勤予定", "次回出勤", "バスト", "ウェスト", "ヒップ", "店舗"]
    existing_columns = [col for col in desired_order if col in df_display.columns]
    df_display = df_display.reindex(columns=existing_columns)
    html_table = df_display.to_html(escape=False, index=False, table_id='resultsTable', classes='display compact stripe hover')

    # f-stringではなく、通常の文字列としてテンプレートを定義
    html_template = """
    <html><head><meta charset="UTF-8"><title>{title}</title>
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.1/css/jquery.dataTables.css">
    <script type="text/javascript" charset="utf8" src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script type="text/javascript" charset="utf8" src="https://cdn.datatables.net/1.13.1/js/jquery.dataTables.js"></script>
    <style>body{{font-family:sans-serif; margin:1.5em;}} table.dataframe th,td{{padding:5px 10px;border:1px solid #ddd; text-align: left; vertical-align: middle;}} thead th{{background-color:#f0f0f0; cursor:pointer;}} .dataTables_wrapper .dataTables_length, .dataTables_wrapper .dataTables_filter {{float: left; margin-right: 20px;}} .dataTables_wrapper .dataTables_info {{clear: both; padding-top: 1em;}} #custom-filters {{padding: 10px; border: 1px solid #ccc; margin-bottom: 1em; border-radius: 5px;}} .filter-container > strong {{display: block; margin-bottom: 8px;}} .filter-row {{display: flex; align-items: center; flex-wrap: wrap; margin-bottom: 5px;}} .filter-row > span {{margin-right: 15px; margin-bottom: 5px;}} #custom-filters select, #custom-filters button {{padding: 4px; border: 1px solid #ccc; border-radius: 3px;}} .action-buttons {{ padding: 0 0 10px 0; }} .action-buttons button {{ padding: 5px 10px; margin-right: 10px; cursor: pointer; border: 1px solid #ccc; border-radius: 3px; background-color: #f0f0f0;}}</style>
    </head>
    <body><h1>{title}</h1><p>各列のヘッダーでソートできます（Shift+クリックで複数ソート可）。画像クリックで次の写真に切り替わります。</p>
    <div id="custom-filters"><div class="filter-container"><strong>カスタムフィルター</strong><div class="filter-row"><span>次回出勤: <select id="min-time"></select> ～ <select id="max-time"></select><button id="reset-time" type="button" style="margin-left: 5px; cursor: pointer;">リセット</button></span><span>年齢: <select id="min-age"></select> ～ <select id="max-age"></select></span><span>ウェスト: <select id="min-waist"></select> ～ <select id="max-waist"></select></span></div><div class="filter-row"><span>口コミ数: <select id="min-reviews"></select> ～ <select id="max-reviews"></select><button id="reset-reviews" type="button" style="margin-left: 5px; cursor: pointer;">リセット</button></span><span>出勤日: <select id="min-workdays"></select> ～ <select id="max-workdays"></select><button id="reset-workdays" type="button" style="margin-left: 5px; cursor: pointer;">リセット</button></span></div></div></div>
    <div class="action-buttons"><button id="filter-checked-btn">チェックしたキャストのみを表示</button><button id="show-all-btn">全てのキャストを表示</button></div>
    {table}
    <script>
    function nextImage(container) {{if (!container.dataset.images) return; const images = JSON.parse(container.dataset.images); if (images.length === 0) return; let currentIndex = parseInt(container.dataset.currentIndex, 10); currentIndex = (currentIndex + 1) % images.length; const imgTag = container.querySelector('img'); imgTag.src = images[currentIndex]; container.dataset.currentIndex = currentIndex;}}
    function setupAgeFilters() {{const ageOptions=['<option value="">指定なし</option>']; for (let i=18; i<=35; i++) {{ageOptions.push(`<option value="${{i}}">${{i}}歳</option>`);}} $('#min-age, #max-age').html(ageOptions.join(''));}}
    function setupTimeFilters() {{const timeOptions=['<option value="">指定なし</option>']; const hourSequence=[6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,0,1,2,3,4,5]; for (const i of hourSequence) {{const hour=i.toString().padStart(2,'0'); timeOptions.push(`<option value="${{hour}}:00">${{hour}}:00</option>`);}} $('#min-time, #max-time').html(timeOptions.join(''));}}
    function setupWaistFilters() {{const waistOptions=['<option value="">指定なし</option>']; for (let i=45; i<=80; i++) {{waistOptions.push(`<option value="${{i}}">${{i}}</option>`);}} $('#min-waist, #max-waist').html(waistOptions.join(''));}}
    function setupReviewFilters() {{const reviewOptions=['<option value="">指定なし</option>']; const values=[1,2,3,5,10,20,30,50,100]; for (const val of values) {{reviewOptions.push(`<option value="${{val}}">${{val}}</option>`);}} $('#min-reviews, #max-reviews').html(reviewOptions.join(''));}}
    function setupWorkdayFilters() {{const workdayOptions=['<option value="">指定なし</option>']; for (let i=0; i<=7; i++) {{workdayOptions.push(`<option value="${{i}}">${{i}}</option>`);}} $('#min-workdays, #max-workdays').html(workdayOptions.join(''));}}
    $(document).ready(function(){{
        let isCheckedFilterActive=false;
        $.fn.dataTable.ext.search.push(function(settings, data, dataIndex) {{
            const ageColIdx=3; const reviewColIdx=7; const workdayColIdx=8; const timeColIdx=12; const waistColIdx=14;
            var minAge=parseInt($('#min-age').val(),10); var maxAge=parseInt($('#max-age').val(),10); var cellAge=parseInt(data[ageColIdx],10);
            var minTime=$('#min-time').val(); var maxTime=$('#max-time').val(); var cellTime=data[timeColIdx]||"";
            var minWaist=parseInt($('#min-waist').val(),10); var maxWaist=parseInt($('#max-waist').val(),10); var cellWaist=parseInt(data[waistColIdx],10);
            var minReviews=parseInt($('#min-reviews').val(),10); var maxReviews=parseInt($('#max-reviews').val(),10); var cellReviews=parseInt(data[reviewColIdx],10);
            var minWorkdays=parseInt($('#min-workdays').val(),10); var maxWorkdays=parseInt($('#max-workdays').val(),10); var cellWorkdays=parseInt(data[workdayColIdx],10);
            if((!isNaN(minAge)&&(isNaN(cellAge)||cellAge<minAge))||(!isNaN(maxAge)&&(isNaN(cellAge)||cellAge>maxAge))) return false;
            if(minTime&&maxTime&&minTime>maxTime){{if(cellTime<minTime&&cellTime>maxTime)return false;}}else{{if((minTime&&cellTime<minTime)||(maxTime&&cellTime>maxTime))return false;}}
            if((!isNaN(minWaist)&&(isNaN(cellWaist)||cellWaist<minWaist))||(!isNaN(maxWaist)&&(isNaN(cellWaist)||cellWaist>maxWaist))) return false;
            if((!isNaN(minReviews)&&(isNaN(cellReviews)||cellReviews<minReviews))||(!isNaN(maxReviews)&&(isNaN(cellReviews)||cellReviews>maxReviews))) return false;
            if((!isNaN(minWorkdays)&&(isNaN(cellWorkdays)||cellWorkdays<minWorkdays))||(!isNaN(maxWorkdays)&&(isNaN(cellWorkdays)||cellWorkdays>maxWorkdays))) return false;
            if(isCheckedFilterActive){{var rowNode=table.row(dataIndex).node(); var isChecked=$(rowNode).find('.row-checkbox').is(':checked'); if(!isChecked)return false;}}
            return true;
        }});
        var table=$('#resultsTable').DataTable({{"pageLength":-1, "lengthMenu":[[10,25,50,100,-1],[10,25,50,100,"全て"]], "order":[], "dom":'<"top"lfi>rt<"bottom"p><"clear">'});
        setupAgeFilters(); setupTimeFilters(); setupWaistFilters(); setupReviewFilters(); setupWorkdayFilters();
        function setDefaultFiltersAndDraw(){{$('#min-age').val('18'); $('#max-age').val('29'); $('#min-waist').val('48'); $('#max-waist').val('60'); const now=new Date(); const startHour=(now.getHours()-3+24)%24; const formattedStart=startHour.toString().padStart(2,'0')+':00'; $('#min-time').val(formattedStart); $('#max-time').val('05:00'); table.draw();}}
        setDefaultFiltersAndDraw();
        $('#min-age, #max-age, #min-time, #max-time, #min-waist, #max-waist, #min-reviews, #max-reviews, #min-workdays, #max-workdays').on('change', function(){{table.draw();}});
        $('#reset-time').on('click', function(){{$('#min-time, #max-time').val(''); table.draw();}});
        $('#reset-reviews').on('click', function(){{$('#min-reviews, #max-reviews').val(''); table.draw();}});
        $('#reset-workdays').on('click', function(){{$('#min-workdays, #max-workdays').val(''); table.draw();}});
        $('#filter-checked-btn').on('click', function(){{isCheckedFilterActive=true; table.draw();}});
        $('#show-all-btn').on('click', function(){{isCheckedFilterActive=false; table.draw();}});
    }});
    </script></body></html>
    """
    # .format() を使って変数を埋め込む
    return html_template.format(title=title_text, table=html_table)


# --- Streamlit UI ---
st.set_page_config(page_title="CityHeaven Scraper", layout="wide")
st.title("🏙️ CityHeaven Scraper")

if 'result_df' not in st.session_state: st.session_state.result_df = None
if 'is_running' not in st.session_state: st.session_state.is_running = False

with st.sidebar:
    st.header("検索条件")
    prefecture_name = st.selectbox("都道府県", options=list(PREFECTURES.keys()), index=0)
    
    # ★★★ ここのコードを修正しました ★★★
    with st.expander("特徴で絞り込み"):
        selected_features = []
        
        st.subheader("年齢")
        for name in AGE_MAP.keys():
            if st.checkbox(name, value=True):
                selected_features.append(name)

        st.subheader("身長")
        for name in HEIGHT_MAP.keys():
            if st.checkbox(name, value=True):
                selected_features.append(name)
        
        st.subheader("バスト")
        for name in BUST_MAP.keys():
            if st.checkbox(name):
                selected_features.append(name)
        
        st.subheader("その他特徴")
        for name in FEATURE_MAP.keys():
            if st.checkbox(name):
                selected_features.append(name)

    st.header("オプション")
    page_limit = st.select_slider("最大取得ページ数", options=['全て', 1, 2, 3, 5, 10, 15, 20], value=3)
    hide_inactive = st.checkbox("出勤未定者を表示しない", value=True)
    debug_mode = st.checkbox("デバッグモード (1ページのみ取得)")
    start_button = st.button("スクレイピング開始", type="primary", disabled=st.session_state.is_running)

if start_button:
    st.session_state.is_running = True
    st.session_state.result_df = None
    selected_typ_codes = [ALL_TYPS[name] for name in selected_features]
    filter_path = f"girl-list/{'-'.join(selected_typ_codes)}/" if selected_typ_codes else "girl-list/"
    params = (PREFECTURES[prefecture_name], filter_path, debug_mode, hide_inactive, None if page_limit == '全て' else int(page_limit))
    st.subheader("処理状況"); progress_bar = st.progress(0); status_text = st.empty()
    result = run_scraper(params, progress_bar, status_text)
    st.session_state.result_df = result
    st.session_state.is_running = False
    st.rerun()

if st.session_state.result_df is not None:
    st.header("スクレイピング結果")
    if not st.session_state.result_df.empty:
        tab1, tab2 = st.tabs(["📊 高機能HTMLレポート", "📈 シンプルな表"])
        with tab1:
            title = f"{prefecture_name} / {'・'.join(selected_features) if selected_features else '特徴指定なし'}"
            html_report = generate_html_report(st.session_state.result_df, title)
            st.components.v1.html(html_report, height=800, scrolling=True)
        with tab2:
            st.dataframe(st.session_state.result_df, column_config={"ギャラリーURL": st.column_config.ImageColumn("Photo"), "プロフィールリンク": st.column_config.LinkColumn("Profile Link")}, use_container_width=True)
        csv = st.session_state.result_df.to_csv(index=False).encode('utf-8-sig')
        st.download_button(label="結果をCSVでダウンロード", data=csv, file_name=f"heaven_data_{prefecture_name}_{int(time.time())}.csv", mime="text/csv")
    else:
        st.warning("条件に合うデータが見つかりませんでした。")
