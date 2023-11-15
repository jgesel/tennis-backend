import os
from flask import Flask, jsonify, request
from flask_cors import CORS, cross_origin
import configs
import psycopg2
import pandas.io.sql as sqlio

port = int(os.environ.get("PORT", 5000))
app = Flask(__name__)
CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
app.config['Access-Control-Allow-Origin'] = '*'

CONNECTION = psycopg2.connect(
      dbname=os.environ.get("DBNAME"),
      user=os.environ.get("DBUSER"),
      password=os.environ.get("PASSWORD"),
      host=configs.os.environ.get("HOST"),
      port=configs.os.environ.get("DBPORT")
  )

print("Connected to DB!")

def get_df_from_database(query, params = None):
  # Establishing the connection

  print(f"Reading Query: \n {query}")
  return sqlio.read_sql_query(query, CONNECTION, params=params)
    
@app.route('/',  methods=['GET'])
def hello_world():
    return 'Web App with Python Flask!'

@app.route('/api/recent', methods=['GET'])
def get_most_recent():
    date = request.args.to_dict()['date']
    recent_q = f'''select "matchID", MAX(p0) AS p0, MAX(p1) AS p1, MAX(matchtime) AS mt
                  from draftkingsnew
                  where DATE(matchtime) = %(date)s
                  group by 1
                  order by 4 DESC;
               '''
    df = get_df_from_database(recent_q, {'date': date})
    recent_list = df[['matchID', 'p0', 'p1']].to_dict('records')
    return jsonify(recent_list)

@app.route('/api/matchtable', methods=['GET'])
def get_match_table():
    match_id = request.args.to_dict()['mid']
    recent_q = f'''select * 
                  from draftkingsnew
                  where "matchID" = %(matchid)s
                '''
    df = get_df_from_database(recent_q, {'matchid': match_id})
    df.drop(columns=['p0', 'p1', 'p0ELO', 'p1ELO', 'matchID', 'matchtime'], inplace=True)
    return df.to_json(orient = 'records')

@app.route('/api/matchdata', methods=['GET'])
def get_match_data():
    match_id = request.args.to_dict()['mid']
    recent_q = f'''select * 
                  from draftkingsnew
                  where "matchID" = %(matchid)s
                '''
    df = get_df_from_database(recent_q, {'matchid': match_id})
    try:
        p0elo = df['p0ELO'].iloc[0]
        p1elo = df['p1ELO'].iloc[0]
        p0 = df['p0'].iloc[0]
        p1 = df['p1'].iloc[0] 
        tournament = df['matchID'].iloc[0].split('__')[0]
        return {'p0elo': str(p0elo), 'p1elo': str(p1elo), 
                'p0': p0, 'p1': p1, 'tournament': tournament}
    except:
        return {}

if __name__ == "__main__":
    app.run(port=port)
