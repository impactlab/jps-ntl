from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_curve
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

features_file = '../data/features-05-cleaned.txt'
labels_file = '../data/meter_to_flag.csv'

df_features = pd.read_csv(features_file)
df_labels = pd.read_csv(labels_file)

# rename the first column of df_features to 'meter_id'
oldname = df_features.columns[0]
df_features.rename(columns={oldname: 'meter_id'},inplace=True)

# make the formatting of the meter ids consistent
df_labels['meter_id_str']=df_labels['meter_id'].astype('string')
df_features['meter_id']=df_features.apply(lambda x: x['meter_id'].strip(' "'),axis=1)
num_features = df_features.shape[1]

df=pd.merge(df_features,df_labels,how='inner',left_on='meter_id', right_on='meter_id_str')

#split into training and test sets
df['is_train'] = np.random.uniform(0, 1, len(df)) <= .5 
train, test = df[df['is_train']==True], df[df['is_train']==False]

# train random forest
features = df.columns[1:num_features]
clf = RandomForestClassifier(n_jobs=2, n_estimators = 1000)
y, _ = pd.factorize(train['lossimpacting'])
clf.fit(train[features], y)

# accuracy 
preds = clf.predict(test[features])
y_tst = test['lossimpacting']
pd.crosstab(y_tst, preds, rownames=['actual'], colnames=['preds'])

scores = clf.predict_proba(test[features])[:,1]
fpr, tpr, thresholds = roc_curve(y_tst, scores)

plt.plot(fpr, tpr)


