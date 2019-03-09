

import numpy as np
import pandas as pd



def otu_files_merge(fpaths, sep=",", delimiter=None, header='infer',
                    names=None, index_col=None, usecols=None,
                    squeeze=False, prefix=None, mangle_dupe_cols=True, dtype=None, engine=None,
                    converters=None, true_values=None, false_values=None,
                    skipinitialspace=False,
                    skiprows=None, skipfooter=0, nrows=None,
                    na_values=None, keep_default_na=True, na_filter=True,
                    verbose=False, skip_blank_lines=True, parse_dates=False,
                    infer_datetime_format=False, keep_date_col=False,
                    date_parser=None, dayfirst=False, iterator=False, chunksize=None,
                    compression='infer', thousands=None, decimal=b'.',
                    lineterminator=None, quotechar='"', quoting=0, doublequote=True,
                    escapechar=None, comment=None, encoding=None, dialect=None,
                    tupleize_cols=None, error_bad_lines=True, warn_bad_lines=True,
                    delim_whitespace=False, low_memory=True, memory_map=False,
                    float_precision=None):

    # create empty final data frame
    df_all = pd.DataFrame()

    for i,fpath in enumerate(fpaths):
        # specify row number(s) to use as header
        if hasattr(header, '__len__') and len(header)>1:
            p_header = header[i]
        else:
            p_header=header

        # specify column names
        if hasattr(names, '__len__') and len(names)>1:
            p_names = names[i]
        else:
            p_names = names

        # specify index column
        if hasattr(index_col, '__len__') and len(index_col)>1:
            p_index_col = index_col[i]
        else:
            p_index_col = index_col

        # specify column to use
        if hasattr(usecols, '__len__') and len(usecols)>1:
            p_usecols = usecols[i]
        else:
            p_usecols = usecols

        # specify skiprows to use
        if hasattr(skiprows, '__len__') and len(skiprows)>1:
            p_skiprows = skiprows[i]
        else:
            p_skiprows = skiprows

        # specify skipfooter to use
        if hasattr(skipfooter, '__len__') and  len(skipfooter)>1:
            p_skipfooter = skipfooter[i]
        else:
            p_skipfooter = skipfooter

        # specify nrows to use
        if hasattr(nrows, '__len__') and len(nrows)>1:
            p_nrows = nrows[i]
        else:
            p_nrows = nrows

        df = pd.read_csv( fpath, sep=sep, delimiter=delimiter, header=None,
                    index_col=p_index_col, usecols=p_usecols,
                    squeeze=squeeze, prefix= "{}.".format(i), mangle_dupe_cols=mangle_dupe_cols,
                    dtype=dtype, engine=engine,converters=converters, true_values=true_values,
                    false_values=false_values,skipinitialspace=skipinitialspace,
                    skiprows=p_skiprows, skipfooter=p_skipfooter, nrows=p_nrows,
                    na_values=na_values, keep_default_na=keep_default_na, na_filter=na_filter,
                    verbose=verbose, skip_blank_lines=skip_blank_lines, parse_dates=parse_dates,
                    infer_datetime_format=infer_datetime_format, keep_date_col=keep_date_col,
                    date_parser=date_parser, dayfirst=dayfirst, iterator=iterator,
                    chunksize=chunksize, compression=compression, thousands=thousands,
                    decimal=decimal, lineterminator=lineterminator, quotechar=quotechar,
                    quoting=quoting, doublequote=doublequote, escapechar=escapechar,
                    comment=comment, encoding=encoding, dialect=dialect,
                    tupleize_cols=tupleize_cols, error_bad_lines=error_bad_lines,
                    warn_bad_lines=warn_bad_lines, delim_whitespace=delim_whitespace,
                    low_memory=low_memory, memory_map=memory_map, float_precision=float_precision)

        # treat colnames as additional

        # create tuple
        if p_names :
            tuples = list(zip(*[ np.array(p_names),df.columns.values]))
        elif p_header:
            tuples = list(zip(*[df.iloc[p_header,:].values, df.columns.values]))
        else:
            print("Not finished yet")
        # drop line with header infromation
        if type(p_header) is int:
            df = df.drop(df.index[p_header])
        index = pd.MultiIndex.from_tuples(tuples, names=['first', 'second'])
        df = pd.DataFrame(df.values, df.index.values, columns=index)

        # concate to existing data frame
        df_all = pd.concat([df_all,df],axis=1,sort=False)


    df_sum = pd.DataFrame()
    for i in list(df_all.columns.levels[0].values):

        if type(prefix) is str:
            column = prefix+"{}".format(i)
        elif prefix is None:
            column = i
        else:
            raise TypeError('"prefix" of type {} not allowed'.format(type(prefix)))

        df_add = pd.DataFrame(df_all[i].astype('double').sum(axis=1),columns = [column])
        df_sum = pd.concat([df_sum, df_add],axis=1,sort=True)

    return(df_sum)