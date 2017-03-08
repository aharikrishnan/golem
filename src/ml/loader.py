import pandas as pd
import MySQLdb as my
import re
import os
from sqlalchemy import create_engine

def short_bn(bn, n=2):
    return ":".join(bn.split("|")[-n:])

def cleanse(str):
    return re.sub(" +"," ",re.sub("[^0-9a-z ]"," ",str.lower())).strip()

def last_n(sentence, w=5):
        return " ".join(cleanse(sentence).split(" ")[-w:])

def word_match(s1,s2,stem=4):
    s1=Set([x[0:stem] for x in cleanse(s1).split(" ") ])
    s2=Set([x[0:stem] for x in cleanse(s2).split(" ") ])
    return 1.0*len(s1 & s2)/len(s1 | s2)

def load_sql(sql):
        conn = my.connect('127.0.0.1','root','root','ml')
        return pd.read_sql(sql, con=conn)

def load_csv(csv):
        return pd.read_csv(csv, sep="\t")

def execute_sql(sql):
        conn = my.connect('127.0.0.1','root','root','ml')
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

def to_sql(df, tbl_name, db='ml'):
        engine = create_engine("mysql+mysqldb://root:"+'root'+"@127.0.0.1/"+db)
        df.to_sql(con=engine, name=tbl_name, if_exists="append", chunksize=1000, index=False)

def print_top_words(model, feature_names, n_top_words):
    for topic_idx, topic in enumerate(model.components_):
        print("Topic #%d:" % topic_idx)
        print(" ".join([feature_names[i]
                        for i in topic.argsort()[:-n_top_words - 1:-1]]))
    print()

