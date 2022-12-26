import re
import pandas as pd
import sqlite3

# Memanggil file csv untuk acuan kata kata alay
kamus_alay = pd.read_csv('new_kamusalay.csv', encoding='latin-1', header=None)
kamus_alay = kamus_alay.rename(columns={0: 'original', 
                                      1: 'replacement'})

# Memanggil file csv untuk acuan stopwrod
kamus_stopword = pd.read_csv('stopwordbahasa.csv', header=None)
kamus_stopword = kamus_stopword.rename(columns={0: 'stopword'})

# Membuat database baru
conn = sqlite3.connect('cleansingresult.db', check_same_thread=False)
conn.row_factory = sqlite3.Row
mycursor = conn.cursor()
print("Successfully created or opened the database")

# Membuat tabel
conn.execute('''CREATE TABLE IF NOT EXISTS data (text varchar(255), text_clean varchar(255));''')
print("Successfully created table")

# Fungsi untuk membersihkan data
def lowercase(text):
    return text.lower()

def remove_unnecessary_char(text):
    text = re.sub('\n',' ',text) # Menghilangkan '\n'
    text = re.sub('rt',' ',text) # Menghilangkan retweet symbol
    text = re.sub('user',' ',text) # Menghilangkan username
    text = re.sub(r'\\x[A-Za-z0-9./]+', '', text) #Menghilangkan \x
    text = re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(http?://[^\s]+))',' ',text) # Menghilangkan URL
    text = re.sub('  +', ' ', text) # Menghilangkan extra spaces
    return text
    
def remove_nonaplhanumeric(text):
    text = re.sub('[^0-9a-zA-Z]+', ' ', text) # Menghilangkan non aplha numeric
    return text

map_kamus_alay = dict(zip(kamus_alay['original'], kamus_alay['replacement']))
def normalize_alay(text):
    return ' '.join([map_kamus_alay[word] if word in map_kamus_alay else word for word in text.split(' ')]) # Mengganti kata kata alay

def remove_stopword(text):
    text = ' '.join(['' if word in kamus_stopword.stopword.values else word for word in text.split(' ')]) #Menghilangkan kata kata yang tidak penting
    text = re.sub('  +', ' ', text) # Menghilangkan extra spaces
    text = text.strip()
    return text

def cleansing_text(text):
    text = lowercase(text) # 1
    text = remove_unnecessary_char(text) # 2
    text = remove_nonaplhanumeric(text) # 3
    text = normalize_alay(text) # 4
    text = remove_stopword(text) # 5
    return text

# Fungsi untuk membersihkan data dari sebuah file
def cleansing_file(input_file):
  column = input_file.iloc[:,0]
  print(column)

  for data in column:
    text_clean = cleansing_text(data)
    a = "insert into data(text,text_clean) values(?,?)"
    b = (data, text_clean)
    mycursor.execute(a, b)
    conn.commit()
    print(data)

# Membuat API untuk data cleansing
from flask import Flask, jsonify

app = Flask(__name__)

from flask import request
from flasgger import Swagger, LazyString, LazyJSONEncoder
from flasgger import swag_from

@app.route('/', methods=['GET'])
def hello_world():
    json_response = { 
        'status_code':200, 
        'description':"untuk melakukan cleansing data tambahkan '/docs' di endpoint", 
        'data': "Welcome to API data cleansing"
        }

    response_data = jsonify(json_response)
    return response_data

app.json_encoder = LazyJSONEncoder
swagger_template = dict(
info = {
    'title': LazyString(lambda: 'API Data Cleansing'),
    'version': LazyString(lambda: '1.0.0'),
    'description': LazyString(lambda: 'Dokumentasi API untuk Membersihkan Data'),
    },
    host = LazyString(lambda: request.host)  
)
swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": 'Text-Cleansing',
            "route": '/docs.json',
        }
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/docs/"
}
swagger = Swagger(app, template=swagger_template,
                  config=swagger_config)

@swag_from("text_cleansing.yml", methods=['POST'])
@app.route('/text-cleansing', methods=['POST'])
def text_cleansing():
    text = request.form.get('text')

    clean_text = cleansing_text(text)

    conn.execute("INSERT INTO data (text, text_clean) VALUES ('" + text + "', '" + clean_text + "')")
    conn.commit()

    json_response = {
        'status_code': 200,
        'description': "Teks yang sudah dibersihkan",
        'data': clean_text,
    }
    
    response_data = jsonify(json_response)
    return response_data

@swag_from("text_cleansing_file.yml", methods=['POST'])
@app.route('/text-cleansing-file', methods=['POST'])
def text_cleansing_file():
    file = request.files['file']

    try:
        data_csv = pd.read_csv(file, encoding='iso-8859-1')
    finally:
        print('This is always executed')
    
    cleansing_file(data_csv)

    a = "select * from data"
    select_data = mycursor.execute(a)
    conn.commit
    data=[
      dict(clean_text=row[1])
    for row in select_data.fetchall()
    ]
    
    return jsonify(data) 
if __name__ == '__main__':
    app.run()         