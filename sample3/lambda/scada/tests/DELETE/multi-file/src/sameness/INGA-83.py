
# coding: utf-8

# In[108]:




"""Elementwise equality test

A simple sameness test will be deployed on 2 arbitrary files. The test will compare 
some arbitrary data columns producing flag columns that indicate that there is element 
level equivalence between the 2 data columns. The test will flag if the 2 are 
equivalent with the following key requirements:

  1. flag only if the elements are identical for a run of more than some threshold, t
  2. flag identical only if elements are equal after truncating the more precise 
     number to the number of significant digits of the least precise.
  3. NaN values will be ignored, i.e. marked as equal if either series 
     contains them. this means that runs will not be broken for NaN values

Examples:
  * "ALIJAR we detected turbine A1, A3, A7 have identical realpower readings 
    between yyyy-mm-dd hh:mm:ss and yyyy-mm-dd hh:mm:ss"
        
Attributes:
    
    
To do:
    * Test with different date ranges
    * Integrate with testing framework
    * How does this interact with other tests?

"""

import pandas as pd
import sys
sys.path.append('/Users/sentient-asoellinger/code/ingestigator-processing/lambda/scada/tests/multi-file/src')
import s3ss_python2 as s3ss

def _rolling_count(val):
    
    if val == _rolling_count.previous:
        
        _rolling_count.count +=1
        
    else:
        
        _rolling_count.previous = val
        
        _rolling_count.count = 1
        
    return _rolling_count.count

def _rolling_index(val):
    
    if val != _rolling_index.previous:
        
        _rolling_index.previous = val
        
        _rolling_index.count += 1
        
    return _rolling_index.count

def _find_sig_figs(number):
    
    return len(str(number).split('.')[-1])

def _truncate(number, n):
    '''
    Truncates/pads a float f to n 
    decimal places without rounding
    '''
    
    int_n = int(n)
    
    s = '{}'.format(number)
    
    if 'e' in s or 'E' in s:
        
        return '{0:.{1}f}'.format(number, int_n)
    
    i, p, d = s.partition('.')
    
    return '.'.join([i, (d + '0' * int_n)[:int_n]])

def _split_mergeddf_header(header):
    
    temp = header.split('-')
    
    return {
        'scada_plant': temp[0],
        'scada_device': temp[1],
        'metric': temp[2],
        'statistic': temp[3],
        'header': header
    }

def _condense_names(name1, name2):
    
    a = set([name1, name2])
    
    return '-'.join(list(a))

def _make_element_equality_base_header(metricB):
    
    #wtg = _condense_names(metricA['scada_plant'] + '+' + metricA['scada_device'],
    #                      metricB['scada_plant'] + '+' + metricB['scada_device'])
    #metric = _condense_names(metricA['metric'], metricB['metric'])
    #stat = _condense_names(metricA['statistic'], metricB['statistic'])
    
    return metricB['scada_plant'] + '+' + metricB['scada_device'] +            '_' + metricB['metric'] + '_' + metricB['statistic']

def _compare_equality(elem1, elem2):
    
    if pd.isnull(elem1) or pd.isnull(elem2):
        
        return 1
    
    if elem1 == elem2:
        
        return 1
    
    return 0

def compare_element_equality(merged_df, min_periods):
    
    fields = [col for col in merged_df.columns if col != 'ts']
    
    combos = set()
    
    for i, fi in enumerate(fields):
        
        for j, fj in enumerate(fields):
            
            if i == j: continue
                
            combos.add(tuple(sorted([fi, fj])))
    
    mask = []

    for combo in combos:
        
        metricA = _split_mergeddf_header(combo[0])
        metricB = _split_mergeddf_header(combo[1])
        
        new_header = _make_element_equality_base_header(metricB)
        
        # use truncated variables
        merged_df[combo[0] + '_sig_figs'] = merged_df[combo[0]].apply(_find_sig_figs)
        merged_df[combo[1] + '_sig_figs'] = merged_df[combo[1]].apply(_find_sig_figs)
        
        #merged_df[[combo[0], combo[0] + '_sig_figs']] \
        #    .apply(lambda x: _print(x[combo[0]], x[combo[0] + '_sig_figs']))
        merged_df['min_sig_figs'] = merged_df[[combo[0] + '_sig_figs',
                                               combo[1] + '_sig_figs']] \
                                             .apply(lambda x: min(x[0], x[1]), axis=1)
        
        merged_df[combo[0] + '_trunc'] = merged_df[[combo[0], 'min_sig_figs']]                                          .apply(lambda x: _truncate(x[0], x[1]), axis=1)
            
        merged_df[combo[1] + '_trunc'] = merged_df[[combo[1], 'min_sig_figs']]                                          .apply(lambda x: _truncate(x[0], x[1]), axis=1)
        
        # check equality
        analysis_header = new_header + '_equality'
        merged_df[analysis_header] =             merged_df[[combo[0] + '_trunc', combo[1] + '_trunc']]             .apply(lambda elems: _compare_equality(elems[0], elems[1]), axis=1)
        
        # take account of the count the number of running flags
        account_header = new_header + '_rolling_count'
        _rolling_count.count = 0
        _rolling_count.previous = None
        merged_df[account_header] = merged_df[analysis_header].apply(_rolling_count)
        
        # give each run a unique id
        _rolling_index.count = 0
        _rolling_index.previous = None
        srs_index = new_header + '_series_index'
        merged_df[srs_index] = merged_df[analysis_header].apply(_rolling_index)
        
        # check for runs that lasted longer than min_periods
        good_periods = merged_df[(merged_df[account_header] >= min_periods) 
                                & merged_df[analysis_header] == 1][srs_index] \
                       .drop_duplicates().as_matrix()
        
        # flag rows that are part of a run longer than min_periods
        flag_header = new_header + '_elementEquality(' + str(min_periods) + ')'
        merged_df[flag_header] = merged_df[srs_index].isin(good_periods)
    
    return merged_df[flag_header]
    #return merged_df

def parse_key(key):
    
    temp  = key.split('/')
    comps = ['data_type', 'step', 'scada_plant', 
             'scada_device', 'metric', 'fn']
    
    out = dict(zip(comps, temp))
    
    pth = out['fn'].split('.')[:-2]
    comps = ['scada_plant', 'scada_device', 
             'metric', 'start', 'end']
    
    out = dict([tuple([k, v]) for k, v in out.iteritems()] + zip(comps, pth))
    out['key'] = key
    
    return out

def pkey_to_column_header(pkey, stat):
    
    return pkey['scada_plant'] + '-'            + pkey['scada_device'] + '-'            + pkey['metric'] + '-'            + stat

def get_and_combine_dfs(bucket, df1, pkey1, pkey2, temp_dir, on, stat):
        
    if not os.path.exists(temp_dir+pkey2['fn']):

        s3ss.get_save_object(bucket=bucket, key=pkey2['key'], 
                             aws_cred='soellingeraj', outfile=temp_dir+pkey2['fn'])
    
    df2 = pd.read_csv(temp_dir+pkey2['fn'], usecols=[on, stat], compression='bz2')
    
    # combine dataframes
    merged_df = df1[['ts', stat]].merge(df2, on=on, how='outer')
    merged_df.index = merged_df[on]
    merged_df = merged_df.drop([on], axis=1)
    
     # set field name convention
    merged_df.columns = [pkey_to_column_header(pkey1, stat), 
                         pkey_to_column_header(pkey2, stat)]
    
    return merged_df

def main(merged_df, min_rows):
    
    return compare_element_equality(merged_df, min_rows)


# In[109]:


if __name__ == '__main__':
    
    _bucket = 'sentient-science-customer-acciona'

    _out_key = 'scada/tested/'
    
    _key_base = 'scada/mapped/'

    _scada_plant = 'ADRANO'

    _key_base = key_base + scada_plant

    _temp_dir = '/Users/sentient-asoellinger/Downloads/temp/'

    _min_hrs = 3

    _min_rows = 3 * 6

    _tgt_metric = 'realpower'

    _tgt_stat = 'avg'

    _files = list(s3ss.get_matching_s3_objects(bucket, 'soellingeraj', 
                                               prefix=key_base))

    _rolling_count.count = 0
    _rolling_count.previous = None

    _rolling_index.count = 0
    _rolling_index.previous = None

    _inventory = {}

    for file in files:

        pkey = parse_key(file['Key'])

        if pkey['metric'] not in _inventory: _inventory[pkey['metric']] = []

        _inventory[pkey['metric']].append(pkey)

    _tested = _inventory[_tgt_metric][0]

    s3ss.get_save_object(bucket=bucket, key=_tested['key'], 
                                 aws_cred='soellingeraj', outfile=temp_dir+_tested['fn'])

    _df1 = pd.read_csv(temp_dir+_tested['fn'], compression='bz2')

    _df1.index = _df1['ts']

    for pkey in _inventory[_tgt_metric][:3]:

        # don't compare _tested to itself
        if pkey == _tested: continue

        # make sure that the time ranges of the files overlap
        if float(_tested['end']) < float(pkey['start']): continue
        if float(_tested['start']) > float(pkey['end']): continue

        # download data to temp and load dataframes
        _merged_df = get_and_combine_dfs(_bucket, _df1, _tested, pkey, 
                                         _temp_dir, 'ts', _tgt_stat)

        # get analysis
        new_df = main(_merged_df, _min_rows)

        # update _df1 with new flag column
        _df1 = _df1.merge(new_df.to_frame(), left_index=True, right_index=True)

        # delete df2 from temp
        os.remove(_temp_dir+pkey['fn'])

    os.remove(_temp_dir+_tested['fn'])
    _df1 = _df1.drop(['ts'], axis=1)
    print 'Finished and cleaned up.'


# In[129]:


_df1


# In[122]:


cols1 = [c for c in _df2.columns if '+' in c]
_df2['elementEqualityAllTrue'] = _df2[cols1].sum(axis=1)
_df2[_df1.elementEqualityAllTrue > 1]

