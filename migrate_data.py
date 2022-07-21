import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:secure_password!12@localhost/capish_dw_testing')
df = pd.read_sql("select * from dim_element", con=engine)
df['media'].fillna('',inplace=True)
df2 = df.groupby(['source_id','element_type','text_id','media'])['media'].count().reset_index(name='count')
columns = ['element_id','source_id','element_type','text_id','is_private_element','is_deleted','question_type','num_possible_answers','text','audio','video', 'image', 'link', 'location', 'embed','loom','youtube']
final_df = pd.DataFrame(columns=columns)

row = {}
ftemp = df2.values[0]

for f in df2.values:
    if((f[0]==ftemp[0] and f[2]!=ftemp[2]) or f[3] == ''):
        
        if len(list(row.values()))!=0:final_df.loc[len(final_df.index)] = list(row.values())
        switcher = {
            'text':False,
            'audio':False,
            'video':0,
            'image':0,
            'link':0,
            'location':0,
            'embed':0,
            'loom':0,
            'youtube':0
        }
        
    if f[3] == '':
        
        currentRow = df[(df.source_id == f[0]) & (df.text_id == f[2]) & (df.media == '')].values[0]
        
        row.update({
            'element_id':currentRow[0],
            'source_id':f[0], 
            'element_type':f[1],
            'text_id':f[2],
            'is_private_element':currentRow[4],
            'is_deleted':currentRow[5], 
            'question_type':currentRow[7], 
            'num_possible_answers':currentRow[8]
        })
        row.update(switcher)
        

    else:
        if(f[3] == 'audio' or f[3] == 'text'):
            switcher.update({f[3]:True})
        else:
            switcher.update({f[3]:f[4]})
        row[f[3]] = switcher.get(f[3])
    ftemp = f
final_df.loc[len(final_df.index)] = list(row.values())
print(final_df)
final_df.to_sql('dim_element_duplicate', con=engine, index=False, if_exists='replace')
    


