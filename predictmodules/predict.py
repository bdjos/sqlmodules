

from fastai.structured import *
from fastai.column_data import *
from predictmodels import accuweathertest
import os
import numpy as np

def predict():

    PATH=os.path.join('..', '..', 'models')
    
    bin_temps = True
    
    df = pd.read_csv(os.path.join(PATH, 'database.csv'))
    df['Date/Time'] = pd.to_datetime(df['Date/Time'])
    df['Weekday'] = df['Date/Time'].apply(lambda x: x.weekday())
    
    # Check for null values and drop
    drop_list = df.isnull().any()
    
    drop_list = list(drop_list.index[drop_list.values])
    
    for item in drop_list:
        df = df.drop(df.index[df[item].isnull()])
    
    drop_list = df.isnull().any()
    
    def temp_binning(temp):
        if temp < 15:
            temp = 15
        else:
            temp = round(temp, 0)
        return temp
    
    if bin_temps == True:
        df['Temp'] = df['Temp'].astype(float)
        df['Dew Point Temp'] = df['Dew Point Temp'].astype(float)
        df = df.drop(df.loc[df['Temp'].isnull()].index)
    
        df['Temp'] = df['Temp'].apply(lambda temp: temp_binning(temp))
        df['Dew Point Temp'] = df['Dew Point Temp'].apply(lambda temp: temp_binning(temp))
    
    
    # Split Data into train/test split
    
    train_split = round(0.99*len(df))
    test_split = len(df)-train_split
    
    joined = df.iloc[:train_split].copy()
    joined_test = df.iloc[train_split:].copy()
    joined_test_df = joined_test.copy()
    joined_test = joined_test.drop(['Ontario Demand'], axis=1)
    
    joined.columns
    
    if bin_temps == True:
        cat_vars = ['Year', 'Month', 'Day', 'Hour', 'Weekday', 'Temp', 'Dew Point Temp']
        contin_vars = ['Rel Hum (%)']
    
    else:
        cat_vars = ['Year', 'Month', 'Day', 'Hour', 'Weekday']
        contin_vars = ['Rel Hum (%)', 'Temp', 'Dew Point Temp']
    
    n = len(joined); n
    
    joined.index = joined['Date/Time']
    joined = joined.drop(['Date/Time'], axis=1)
    
    joined_test.index = joined_test['Date/Time']
    joined_test = joined_test.drop(['Date/Time'], axis=1)
    
    dep = 'Ontario Demand'
    joined = joined[cat_vars+contin_vars+[dep]].copy()
    
    joined_test[dep] = 0
    joined_test = joined_test[cat_vars+contin_vars+[dep]].copy()
    
    joined.head()
    
    for v in cat_vars: joined[v] = joined[v].astype('category').cat.as_ordered()
    
    apply_cats(joined_test, joined)
    
    for v in contin_vars:
        joined[v] = joined[v].fillna(0).astype('float32')
        joined_test[v] = joined_test[v].fillna(0).astype('float32')
    
    samp_size = n
    joined_samp = joined
    
    joined_samp.head()
    
    df, y, nas, mapper = proc_df(joined_samp, 'Ontario Demand', do_scale=True)
    yl = np.log(y)
    
    df_test, _, nas, mapper = proc_df(joined_test, 'Ontario Demand', do_scale=True,
                                      mapper=mapper, na_dict=nas)
    
    train_ratio = 0.75
    #train_ratio = 0.9
    train_size = int(samp_size * train_ratio); train_size
    val_idx = list(range(train_size, len(df)))
    
    def inv_y(a): return np.exp(a)
    
    def exp_rmspe(y_pred, targ):
        targ = inv_y(targ)
        pct_var = (targ - inv_y(y_pred))/targ
        return math.sqrt((pct_var**2).mean())
    
    def mse(y_pred, targ):
        var = (targ-y_pred)**2
        return var.mean()
    
    max_log_y = np.max(yl)
    y_range = (0, max_log_y*1.2)
    
    
    md = ColumnarModelData.from_data_frame(PATH, val_idx, df, yl.astype(np.float32), cat_flds=cat_vars, bs=128,
                                           test_df=df_test)
    
    
    
    cat_sz = [(c, len(joined_samp[c].cat.categories)+1) for c in cat_vars]
    
    
    
    emb_szs = [(c, min(50, (c+1)//2)) for _,c in cat_sz]
    
    m = md.get_learner(emb_szs, len(df.columns)-len(cat_vars),
                       0.04, 1, [1000,500], [0.001,0.01], y_range=y_range)
    #lr = 1e-3
    
    m.load(os.path.join(PATH, 'val0'))
    x, y=m.predict_with_targs()
    exp_rmspe(x,y)
    pred_test=m.predict(True)
    pred_test = np.exp(pred_test)
    joined_test['kW predicted'] = pred_test
    joined_test_df = joined_test_df.set_index(joined_test_df['Date/Time'])
    joined_test_df.head()
    joined_test['kW actual'] = joined_test_df['Ontario Demand']
    
    df_forecast = accuweathertest.get_weather() #names=['Date', 'Time', 'Temp', 'Dew Point Temp', 'Rel Hum (%)'])
    
    df_forecast['Date/Time'] = pd.to_datetime(df_forecast['Date/Time'])
    
    
    df_forecast['Weekday'] = df_forecast['Date/Time'].apply(lambda x: x.weekday())
    
    df_forecast.set_index('Date/Time', drop=True, inplace=True)
    
    
    if bin_temps == True:
        df_forecast['Temp'] = df_forecast['Temp'].astype(float)
        df_forecast['Dew Point Temp'] = df_forecast['Dew Point Temp'].astype(float)
        df_forecast = df_forecast.drop(df.loc[df['Temp'].isnull()].index)
    
        df_forecast['Temp'] = df_forecast['Temp'].apply(lambda temp: temp_binning(temp))
        df_forecast['Dew Point Temp'] = df_forecast['Dew Point Temp'].apply(lambda temp: temp_binning(temp))
    
    joined_test = df_forecast[cat_vars + contin_vars].copy()
    
    apply_cats(joined_test, joined)
    
    
    
    df_test, _, _, _ = proc_df(joined_test, None, do_scale=True, na_dict=nas, mapper=mapper)
    
    model = m.model
    model.eval()
    
    preds = []
    
    for i in range(len(df_test)):
        test_record = df_test.iloc[i]
        cat = test_record[cat_vars].values.astype(np.int64)[None]
        contin = test_record.drop(cat_vars).values.astype(np.float32)[None]
    
        prediction = to_np(model(V(cat), V(contin)))
        prediction = np.exp(prediction)
        print(f"Prediction: {prediction}")
        preds.append(prediction[0][0])
    
    df_forecast['Predicted Demand'] = preds
    df_forecast = df_forecast.reset_index()
    
#    df_forecast.to_csv('predictions.csv')
    return df_forecast
    
if __name__ == '__main__':
    predict()


