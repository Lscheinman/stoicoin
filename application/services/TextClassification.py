import tensorflow as tf
import pandas as pd
import numpy as np
import os

cwd = os.getcwd()
if 'C:\\' in cwd:
    dataURL = '%s\\data\\sets\\' % cwd
else:
    dataURL = '%s\data\sets' % cwd

# Can change this to based on data or keep it set
bucketCount = 8

for f in os.listdir(dataURL):
    if '.csv' in f:
        data = pd.read_csv(dataURL + f)
        norm_cols = data.columns
        data[norm_cols] = data[norm_cols].apply(lambda x: (x - x.min()) / (x.max()-x.min()))
        featureCols = []
        
        # FEATURE ENGINEERING
        # Create the feature columns
        for col in norm_cols:
            fCol = {'col' : col,
                    'f_col_num' : tf.feature_column.numeric_column(col),
                    'd_type' : data[col].dtype,
                    'max' : data[col].max(),
                    'min' : data[col].min()
                    }
            # Set bounds for feature buckets within a column
            if fCol['d_type'] == 'float64': 
                # Make uniform buckets based on bucketCount
                div = (fCol['max'] - fCol['min'])/bucketCount
                bounds = [fCol['min']]
                i = 1
                while i < bucketCount:
                    bounds.append(bounds[i-1] + div)
                    i+=1
                
                fCol['f_col_bucket'] = tf.feature_column.bucketized_column(fCol['f_col_num'], bounds)

            featureCols.append(fCol)
                
        print(featureCols)
        
        # Create the categorical columns based on hash, vocab list, or identity
        
        
        featureCols.append({'col' : 'ageBucketized', 'f_col' : tf.feature_column.bucketized_column(col, boundaries)})
        # If the column contains continuous data then use bucketized features based on histogram segmentations and number of buckets


