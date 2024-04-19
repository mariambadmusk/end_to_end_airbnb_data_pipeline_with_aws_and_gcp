import pandas as pd
import numpy as np
from langdetect import detect
import langdetect.lang_detect_exception
from deep_translator import GoogleTranslator


# Sample data
data = {
    'Product': ['NA', 'None', 'Sapatos', 'Dress', 'Fan'],
    'Quantity': [100, 75, 50, 120, 90],
    'Unit Price (R$)': [25.00, 40.00, 80.00, 60.00, 100.00],
    'Total Sales (R$)': [2500.00, 3000.00, 4000.00, 7200.00, 9000.00],
    'Comments': ['Produto muito popular', 'NaN', 'Cliente satisfeito', 'Promoção especial', 'None']
}

def detect_lang(row):
    try:
        detect(row)
    except langdetect.lang_detect_exception.LangDetectException:
        pass
def translate_row(row):
    translation = GoogleTranslator(source = 'auto', target='en').translate(row)
    return translation

# Create DataFrame
sales_df = pd.DataFrame(data)
sales_df_copy = sales_df.copy()
non_english = sales_df_copy.map(lambda x: isinstance(x, str) and detect_lang != 'en')
non_english_indices = non_english[non_english].stack().index.tolist()

values = [sales_df_copy.iloc[row_idx, sales_df_copy.columns.get_loc(col_name)] for row_idx, col_name in non_english_indices ]

print(values)







            



