from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_curve, roc_auc_score
from sklearn.cross_validation import cross_val_score
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

split_meters = True
drop_dup = True

features_file = '../data/features-4-1-cleaned.txt'
events_file = '../data/events-3-31-2-cleaned.txt'
labels_file = '../data/meter_to_flag.csv'

df_features = pd.read_csv(features_file)
df_events = pd.read_csv(events_file)
df_labels = pd.read_csv(labels_file)


# rename the first column of df_features to 'meter_id'
oldname = df_features.columns[0]
df_features.rename(columns={oldname: 'meter_id'},inplace=True)

oldname = df_features.columns[1]
df_features.rename(columns={oldname: 'invest_id'},inplace=True)

oldname = df_labels.columns[1]
df_labels.rename(columns={oldname: 'invest_id'}, inplace=True)

# Drop duplicate columns
df_labels.drop_duplicates('invest_id', inplace=True)

# make the formatting of the meter ids consistent
df_labels['meter_id_str']=df_labels['meter_id'].astype('string')
df_features['meter_id']=df_features.apply(lambda x: x['meter_id'].strip(' "'),axis=1)

features_size = df_features.shape[1] 
events_size = df_events.shape[1]

df_ev_fe = pd.merge(df_features, df_events, how='inner', left_on = 'invest_id', right_on = 'invest_id')
df=pd.merge(df_ev_fe,df_labels,how='inner',left_on='invest_id', right_on='invest_id')

#split into training and test sets
if split_meters:
    meter_ids = df['meter_id_str'].unique()
    meter_ids_sub = meter_ids[np.random.uniform(0, 1, len(meter_ids)) <= .5]
    train, test = df[df['meter_id_str'].isin(meter_ids_sub)], df[~df['meter_id_str'].isin(meter_ids_sub)]
else:
    df['is_train'] = np.random.uniform(0, 1, len(df)) <= .5 
    train, test = df[df['is_train']==True], df[df['is_train']==False]    

# train random forest
features_range = range(2, features_size) + range(features_size + 1, features_size + events_size - 2)
features = df.columns[features_range]
clf = RandomForestClassifier(n_jobs=2, n_estimators = 1000)
y, _ = pd.factorize(train['lossimpacting'])

df_train = train[features]
df_test = test[features]

df_train = df_train.replace([np.inf, -np.inf], np.nan)
df_train.fillna(0., inplace = True)

df_test = df_test.replace([np.inf, -np.inf], np.nan)
df_test.fillna(0., inplace = True)
    
clf.fit(df_train, y)

# accuracy 
preds = clf.predict(df_test)
y_tst = test['lossimpacting']
pd.crosstab(y_tst, preds, rownames=['actual'], colnames=['preds'])

scores = clf.predict_proba(df_test)[:,1]
fpr, tpr, thresholds = roc_curve(y_tst, scores)
auc = roc_auc_score(y_tst, scores)

plt.plot(fpr, tpr)

a=2



