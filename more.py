
# %%
#detect dataframes columns with
def detect_lang(row):
    try:
        detect(row)
    except langdetect.lang_detect_exception.LangDetectException:
        pass

# %%
#detect dataframes columns with non-english
def detect_lang_df(df):
    try:
        col_not_english = {}

        for col in df.columns: 
            not_english_mask = df[col].apply(lambda x: isinstance(x, str) and detect(x) != 'en')                                                                                  
            col_not_english[col] = not_english_mask.any()
    except langdetect.lang_detect_exception.LangDetectException:
        pass

    return col_not_english

# %%
#apply detect values function
print(detect_lang_df(raw_data['losAngelesData']))

# %%
#function to translate non-english to english text
def translate_row(text):
    translate_client = translate.Client()

    if isinstance(text, bytes):
        text = text.decode("utf-8")
    target = 'en'
    result = translate_client.translate(text, target_language=target)

    return result["translatedText"]


# %%
bool_listing = raw_data['miamiData']['Listing Type'].map(lambda x: detect_lang(x) == 'en' if isinstance(x, str) else False)


# %%
def replace_non_english(df):
    def replace_cell(value):
        if pd.isna(value) == True:
            return value
        if not isinstance(value, str):
            return value
        if value.isalnum() == True:
            return value
        if value == '':
            return value
        if detect_lang(value) != 'en':
            translation = translate_row(value)
            return translation
        else:
            return value

    return  df.map(replace_cell)

# %%
            if detect_lang(translation) != 'en':
                for loc in ['City', 'Zipcode', 'State']:
                    if pd.notnull(df[loc]) and pd.notnull(df['Bedrooms']):
                        df['Listing Title'] = str(df.loc['Bedrooms']) + ' ' + str(df.at[idx, loc])
                        break

# %%
#Use Google Geocoding API to fill the zipcode column in torontoData dataframe
url_range= 'https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},-{lang}&key={api}'

for idx, row in raw_data['torontoData'].itterows():
        lat = row['Latitutde']
        lang = row['Longitude']
        if not pd.isna(lat) == False and not pd.isna(lang) == False:
            url = url_range.format(lat = lat, lang = lang, api = google_map_api )
            response = requests.get(url)
            data = response.json()
            postal_code = next(([component['long_name'] for result in url['results'] for component in result['address_components'] if 'postal_code' in component['types']]), None)
            raw_data['torontoData'].at[idx, ['Zipcode']] = postal_code


def replace_non_english(df):
    def replace_cell(value):
        if pd.isna(value) == True:
            return value
        if not isinstance(value, str):
            return value
        if value.isalnum() == True:
            return value
        if value == '':
            return value
        if detect_lang(value) != 'en':
            translation = translate_row(value)
            return translation
        else:
            return value

    return  df.map(replace_cell)


#create new listings
if detect_lang(translation) != 'en':
    for loc in ['City', 'Zipcode', 'State']:
        if pd.notnull(df[loc]) and pd.notnull(df['Bedrooms']):
            df['Listing Title'] = str(df.loc['Bedrooms']) + ' ' + str(df.at[idx, loc])
                break

#remove emojis 

#remove all emoji characters
def remove_emoji(df, df_name):
    emoji_patterns = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002700-\U000027BF"  # Dingbats
        u"\U00002600-\U000026FF"  # Miscellaneous Symbols
        u"\U00002B00-\U00002BFF"  # Miscellaneous Symbols and Arrows
        u"\U0001F100-\U0001F1FF"  # Enclosed Alphanumeric Supplement
        u"\U0001F200-\U0001F2FF"  # Enclosed Ideographic Supplement
        u"\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        u" \U000025A0-\U000025FF" # Geometric Shapes
         "]+", flags = re.UNICODE)
    
    df = df.map(lambda x: emoji_patterns.sub(r' ', x) if isinstance(x, str) else x)
    
    print(f'All emojis removed from {df_name}')

    return df 