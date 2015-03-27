#!/usr/bin/env python
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_curve
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

features_file = './features.csv'
labels_file = '/data/labels/meter_to_flag.csv'

df_features = pd.read_csv(features_file)
num_features = df_features.shape[1]
df_labels = pd.read_csv(labels_file)

df=pd.merge(df_features,df_labels,how='inner',left_on='id', right_index=True)

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
print pd.crosstab(y_tst, preds, rownames=['actual'], colnames=['preds'])

scores = clf.predict_proba(test[features])[:,1]
fpr, tpr, thresholds = roc_curve(y_tst, scores)

plt.plot(fpr, tpr)
plt.savefig('roc.png')

