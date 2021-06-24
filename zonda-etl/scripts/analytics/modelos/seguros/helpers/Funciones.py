import pandas as pd
import numpy as np

from sklearn.metrics import auc
from sklearn.metrics import confusion_matrix
from sklearn.metrics import roc_curve
from sklearn.metrics import log_loss


def eda_univariado_cat(df,lista_categoricas):
    
    def entropy(values,n):
    
        return (np.sum(values * np.log(values)) * (-1)) * (1/math.log(n))
    
    dict_results = {}
    
    for col in lista_categoricas:
        
        dict_col = {}
        
        df_col = df[col].copy()
        total_rows = df_col.shape[0]
        dict_col['porcentaje_NA'] = (df_col.isna().sum()) / total_rows
        dict_col['moda'] = df_col.mode()
        dict_col['cant_unique_val'] = df_col.nunique()
        
        frec_table = df_col.value_counts(normalize = True).reset_index().rename(columns = {'index':'col',col:'frec'})
        dict_col['frec_mean'] = frec_table['frec'].mean()
        dict_col['frec_median'] = frec_table['frec'].median()
        dict_col['frec_std'] = frec_table['frec'].std()
        dict_col['frec_max'] = frec_table['frec'].max()
        dict_col['frec_min'] = frec_table['frec'].min()
        
        dict_col['entropia'] = entropy(frec_table['frec'],dict_col['cant_unique_val'])
        
        dict_results[col] = dict_col
        
    return dict_results


def eda_univariado_cont(df,lista_continuas, guardar_graficos=False ,path_graficos = None):
    
    dict_results = {}
    
    for col in lista_continuas:
        dict_col = {}
        
        df_col = df[col].copy()
        total_rows = df_col.shape[0]
        dict_col['porcentaje_NA'] = (df_col.isna().sum()) / total_rows
        dict_col['media'] = df_col.mean()
        dict_col['mediana'] = df_col.median()
        dict_col['std'] = df_col.std()
        
        dict_col['quantiles'] = df_col.quantile(np.arange(0.0, 1.1, 0.1)).to_list()
        
        ## GRAFICOS
        ## FIN GRAFICOS
        
        ## eliminando outliers
        
        limite_inf = dict_col['media'] - (dict_col['std'] * 3)
        limite_sup = dict_col['media'] + (dict_col['std'] * 3)
        df_col_out = df_col[((df_col>limite_inf) & (df_col<limite_sup))]
        
        dict_col['so_3std_prop'] = df_col_out.shape[0]/total_rows
        
        if(dict_col['so_3std_prop']>.8):
            dict_col['so_media'] = df_col_out.mean()
            dict_col['so_mediana'] = df_col_out.median()
            dict_col['so_std'] = df_col_out.std()

            dict_col['so_quantiles'] = df_col_out.quantile(np.arange(0.0, 1.1, 0.1)).to_list()
        
        dict_results[col] = dict_col
        
    return dict_results

def eda_univariado(df,lista_categoricas,lista_continuas,guardar_graficos=False):
    
    results = {}
    results['categoricas'] = eda_univariado_cat(df,lista_categoricas)
    results['continuas'] = eda_univariado_cont(df,lista_continuas,guardar_graficos)
    
    
    return results


def calcula_metricas(preds,realidad):
    
    #Generales
    fpr, tpr, thresholds = roc_curve(realidad, preds)
    auc_score = auc(fpr, tpr)
    gini_coef = 2*auc_score -1
    logloss_score = log_loss(realidad,preds)
    
    cortes = np.arange(0.0, 1.01, 0.01)
    metricas_pc = []
    #por punto de corte
    for pc in cortes:
        preds_pc = (preds>=pc).astype(int)
        conf_mat = confusion_matrix(realidad,preds_pc)
        true_negatives = conf_mat[0][0]
        false_negatives = conf_mat[1][0]
        true_positives = conf_mat[1][1]
        false_positives = conf_mat[0][1]
        acc = (true_negatives + true_positives) / (true_negatives + true_positives + false_negatives + false_positives)
        precision = true_positives / (true_positives + false_positives)
        recall = true_positives / (true_positives + false_negatives)
        fpr = false_positives / (false_positives + false_negatives)
        specificity = true_negatives / (true_negatives + false_positives)
        f1_score = 2 * (recall * precision) / (recall + precision)
        
        metricas_pc.append({'punto_corte':pc,'metrica':'tn','valor':round(true_negatives,4)})
        metricas_pc.append({'punto_corte':pc,'metrica':'fn','valor':round(false_negatives,4)})
        metricas_pc.append({'punto_corte':pc,'metrica':'tp','valor':round(true_positives,4)})
        metricas_pc.append({'punto_corte':pc,'metrica':'fp','valor':round(false_positives,4)})
        metricas_pc.append({'punto_corte':pc,'metrica':'accuracy','valor':round(acc,4)})
        metricas_pc.append({'punto_corte':pc,'metrica':'precision','valor':round(precision,4)})
        metricas_pc.append({'punto_corte':pc,'metrica':'recall','valor':round(recall,4)})
        metricas_pc.append({'punto_corte':pc,'metrica':'fpr','valor':round(fpr,4)})
        metricas_pc.append({'punto_corte':pc,'metrica':'specificity','valor':round(specificity,4)})
        metricas_pc.append({'punto_corte':pc,'metrica':'f1','valor':round(f1_score,4)})
        
    #Se agregan las generales
    metricas_pc.append({'punto_corte':0,'metrica':'gini_coef','valor':round(gini_coef,4)})
    metricas_pc.append({'punto_corte':0,'metrica':'auc','valor':round(auc_score,4)})
    metricas_pc.append({'punto_corte':0,'metrica':'logloss','valor':round(logloss_score,4)})
    
    total_positivos =  realidad.sum()
    total = realidad.shape[0]
    total_negativos = total - total_positivos
    
    #Metricas que dependen del ordenamiento
    real_pred = pd.DataFrame({'preds':preds,'real':realidad})
    real_pred.sort_values(by = 'preds', ascending = False ,inplace = True)
    decile_q = np.ceil(real_pred.shape[0]/10)
    real_pred.reset_index(drop=True,inplace=True)
    real_pred.reset_index(inplace = True)
    real_pred.rename(columns = {'index':'order'}, inplace=True)
    real_pred['group'] = real_pred['order'] // decile_q +1
    
    deciles = real_pred.groupby(by = 'group')[['real','preds']].agg({'real':['sum','count'],'preds':['min','max','mean']})
    deciles.columns = ['cant_p','cant','preds_min','preds_max','preds_mean']
    deciles.reset_index(inplace=True)
    deciles['cant_n'] = deciles['cant'] - deciles['cant_p'] 
    deciles['porc_p'] = deciles['cant_p'] / total_positivos
    deciles['porc_n'] = deciles['cant_n'] / total_negativos
    deciles['porc_pop'] = deciles['cant'] / total
    acums = deciles[['porc_p','porc_n','porc_pop']].cumsum(axis = 0).rename(columns = {'porc_p':'acum_p','porc_n':'acum_n','porc_pop':'acum_pop'})
    deciles = pd.concat([deciles,acums],axis=1)
    
    deciles = deciles.melt(id_vars = ['group'], value_vars = ['cant_p','cant','cant_n','porc_p','porc_n','porc_pop','acum_p','acum_n','acum_pop','preds_min','preds_max','preds_mean'],var_name='metrica',value_name='valor')
    deciles.rename(columns = {'group':'punto_corte'},inplace = True)
    
    metricas_df = pd.concat([pd.DataFrame(metricas_pc),deciles],axis=0,ignore_index=True)
    
    return metricas_df