import json
import csv
import pandas
import elasticsearch
import os
import sys
import datetime
from timeit import default_timer
import MainMongo
import MainPostgreSQL
import MainELK

def main():
    while True:

        selection=""
        print("************************************************************\n")
        print("                 ***  Select Database Type  ***\n")
        print("MongoDB                                      -> Press 1   --")
        print("PostgreSQL                                   -> Press 2   --")
        print("ElasticSearch                                -> Press 3   --")
        print("MongoDB      + Elasticsearch                 -> Press 4   --")
        print("MongoDB      + PostgreSQL                    -> Press 5   --")
        print("PostgreSQL   + ElasticSearch                 -> Press 6   --")
        print("MongoDB      + PostgreSQL    + ElasticSearch -> Press 7   --")
        print("\n************************************************************\n")
        selection=input("Select One/s       --")
        if selection.upper()=="1":
            #call mongodb functions
            MainMongo.main()
            
        elif selection.upper()=="2":
            #call postgresql functions
            MainPostgreSQL.main()
            
            
        elif selection.upper()=="3":
            #call elasticsearch functions
            MainELK.main()
            
        elif selection.upper()=="4":
            #call mongodb functions
            #call postgresql functions
            MainMongo.main()
            MainELK.main()

        elif selection.upper()=="5":
            #call mongodb functions
            #call elasticsearch functions
            MainMongo.main()
            MainPostgreSQL.main()

        elif selection.upper()=="6":
            #call postgresql functions
            #call elasticsearch functions
            MainPostgreSQL.main()
            MainELK.main()

        elif selection.upper()=="7":
            #call mongodb functions
            #call postgresql functions
            #call elasticsearch functions
            MainMongo.main()
            MainPostgreSQL.main()
            MainELK.main()
        os.system("pause")

if __name__=="__main__":
    main()