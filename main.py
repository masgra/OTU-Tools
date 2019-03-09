
import numpy as np
import pandas as pd
import Tools as OTUtools


if __name__ == "__main__":

    ## ---------------  Read file to dataframe -------------------------------

    #  locations of the file paths
    folder  = "C:/Users/Martin/Nextcloud/Home/Data/MT_Data-122018/readcounts/"


    ##  Creating a list with paths of the files to process

    #  levels to read in
    levels = [1,2,3]

    #  path to file with file paths and levels
    filepath = folder+"filepath.csv"

    #  get paths and according levels from file
    paths = pd.read_csv(filepath, sep="\t", header=0)

    #  names of samples. Samples with equal names will be summed.
    sample_names = [[1, 2, 3, 5, 6, 7, 8, 9],
                 [4, 4, 4, 10, 10, 10, 11, 11, 11],
                 [12, 13, 14, 15, 16, 17, 18, 19],
                 [20, 21, 22, 25, 25, 26, 27]]

    # handle description
    split_from_index = [False, True, True]
    delete_description = [False, True, True]


    for count,i in enumerate(levels):

        #  get paths for specified level
        pathlist = list(folder + paths.loc[paths.iloc[:, 1] == i].iloc[:, 0])

        #  read in files and merge per level
        df_OTU = OTUtools.otu_files_merge(pathlist, sep= "\t", header=0 , index_col=0, names=sample_names, prefix= "S-")

        #  split description to separate column
        if split_from_index[count]:
            df_OTU['Description'] = df_OTU.index.str.split(pat=None, n=1).str[1]
            df_OTU.index = df_OTU.index.str.split(pat=None, n=1).str[0]

        #  split description to separate column
        if delete_description[count]:
            del df_OTU['Description']

        #  store merged OTU table
        df_OTU.to_csv(folder+ "summary_L{}.csv".format(i), sep=";")

    print("done")










