# aisles : aisle_id | aisle
# departments : department_id | department
# orders_products (merge prior + train): order_id | product_id | add_to_cart_order | reordered
# orders : order_id | user_id | eval_set | order_number | order_dow | order_hour_of_day | days_since_prior_order
# products : product_id | product_name | aisle_id | department_id

import pandas as pd

AISLES_CSV_FILEPATH = 'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\datasCSV\\aisles.csv'
DEPARTMENTS_CSV_FILEPATH = 'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\datasCSV\\departments.csv'
ORDER_PRODUCTS_PRIOR_CSV_FILEPATH = 'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\datasCSV\\order_products__prior.csv'
ORDER_PRODUCTS_TRAIN_CSV_FILEPATH = 'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\datasCSV\\order_products__train.csv'
ORDERS_CSV_FILEPATH = 'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\datasCSV\\orders.csv'
PRODUCTS_CSV_FILEPATH = 'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\datasCSV\\products.csv'


# import data from csv to pandas dataframe
def import_data_from(source_data_file):
    return pd.read_csv(source_data_file)


# fonction pour ranked en desc
def add_rank_desc_column(dataframe, orderby_column):
    dataframe = dataframe.sort_values(orderby_column, ascending=False).reset_index(drop=True)
    dataframe['rank'] = dataframe[orderby_column].rank(ascending=False)
    dataframe['rank'] = dataframe['rank'].astype(int)
    return dataframe


class DataParser:

    def __init__(self):
        self.__departments_df = pd.DataFrame()
        self.__order_products_prior_df = pd.DataFrame()
        self.__order_products_train_df = pd.DataFrame()
        self.__products_df = pd.DataFrame()
        self.__aisles_df = pd.DataFrame()

        # transformed dataframes
        self.__order_products_merged_df = pd.DataFrame()
        self.__orders_df = pd.DataFrame()
        self.__products_merged_df = pd.DataFrame()

    # chargement des données des fichiers csv dans les attributs de la classe
    def set_all_attributes(self):
        self.__aisles_df = import_data_from(AISLES_CSV_FILEPATH)
        self.__departments_df = import_data_from(DEPARTMENTS_CSV_FILEPATH)
        self.__order_products_prior_df = import_data_from(ORDER_PRODUCTS_PRIOR_CSV_FILEPATH)
        self.__order_products_train_df = import_data_from(ORDER_PRODUCTS_TRAIN_CSV_FILEPATH)
        self.__orders_df = import_data_from(ORDERS_CSV_FILEPATH)

        # on ne conserve que les lignes avec eval_set != 'test'
        self.__orders_df = self.__orders_df.loc[self.__orders_df['eval_set'] != 'test']

        self.__products_df = import_data_from(PRODUCTS_CSV_FILEPATH)
    
    # merge_* --> regroupement des données pour supprimer les initules

        # aisle + department + product
    def merge_products_aisles_departments_dfs(self):
        self.__products_merged_df = self.__products_df.merge(self.__aisles_df, how='inner', on='aisle_id').merge(self.__departments_df, how='inner', on='department_id')
        
        # order_products__prior.csv + order_products__train.csv + products.csv
    def merge_prior_and_train_dfs(self):
        self.__order_products_merged_df = pd.concat([self.__order_products_prior_df, self.__order_products_train_df], ignore_index=True, sort=False)
        self.__order_products_merged_df = self.__order_products_merged_df.merge(self.__products_df, how='inner', on='product_id')
    

    def get_most_sold_products(self):
        sold_products = self.__order_products_merged_df[['product_id', 'order_id']]
        
        products = self.__products_df[['product_id', 'product_name']]
        sold_products = sold_products.merge(products, how='inner', on='product_id')

        sold_products_count = sold_products.groupby(['product_id', 'product_name'], as_index=False).count()
        
        sold_products_count.rename(columns={"order_id": "sales"}, inplace=True)

        most_sold_products = add_rank_desc_column(sold_products_count, 'sales')


        return most_sold_products
    
    
    def get_all_organic_products(self):
        products = self.__products_df[['product_id', 'product_name']]

        organic_products = products[products['product_name'].str.contains('Organic')]
        # print(organic_products)
        return organic_products


    def get_all_non_organic_products(self):
        products = self.__products_df[['product_id', 'product_name']]

        non_organic_products = products[~products['product_name'].str.contains('Organic')]
        # print(non_organic_products)
        return non_organic_products

    def get_most_performant_aisles(self):
        # ranking desc des produits vendus
        most_sold_products = self.get_most_sold_products()

        # merge avec __products_merged_df pour récupérer les infos des aisles
        most_sold_products_with_aisle = most_sold_products.merge(self.__products_merged_df, how='inner', on='product_id')
        most_sold_products_with_aisle = most_sold_products_with_aisle[['aisle_id', 'aisle', 'sales']]

        # group by aisle
        total_sales_by_aisle = most_sold_products_with_aisle.groupby(['aisle_id', 'aisle'], as_index=False).sum()

        most_performant_aisles = add_rank_desc_column(total_sales_by_aisle, 'sales')

        return most_performant_aisles

    def get_most_performant_categories(self):
        most_sold_products = self.get_most_sold_products()
        most_sold_products_with_department = most_sold_products.merge(self.__products_merged_df, how='inner', on='product_id')
        most_sold_products_with_department = most_sold_products_with_department[['department_id', 'department', 'sales']]

        # group by category
        total_sales_by_department = most_sold_products_with_department.groupby(['department_id', 'department'], as_index=False).sum()

        most_performant_departments = add_rank_desc_column(total_sales_by_department, 'sales')

        return most_performant_departments
    
    def get_products_bought_multiple_times(self):
        orders = self.__order_products_merged_df
        
        products_bought_multiple_times = orders.loc[orders['reordered'] == 1]
        products_bought_multiple_times = products_bought_multiple_times[['product_id', 'product_name', 'reordered']]

        products_most_reordered = products_bought_multiple_times.groupby(['product_id', 'product_name'], as_index=False).count()

        products_most_reordered.rename(columns={"reordered": "number_of_times_reordered"}, inplace=True)
        products_most_reordered = add_rank_desc_column(products_most_reordered, 'number_of_times_reordered')

        return products_most_reordered
    
    def get_products_bought_once_only(self):
        orders = self.__order_products_merged_df
        
        products_bought_once = orders.loc[orders['reordered'] == 0]
        products_bought_once = products_bought_once[['product_id', 'product_name', 'reordered']]

        products_bought_once = products_bought_once.groupby(['product_id', 'product_name'], as_index=False).count()
        products_bought_once.rename(columns={"reordered": "number_of_orders"}, inplace=True)
        products_bought_once = add_rank_desc_column(products_bought_once, 'number_of_orders')

        # get products bought multiple times
        products_bought_multiple_times = self.get_products_bought_multiple_times()[['product_id', 'product_name']]

        # delete products bought multiple times
        df_left_join = products_bought_once.merge(products_bought_multiple_times, how='left', on='product_id')

        # récupère ceux qui sont NULL, càd ceux qui n'ont aucune correspondance avec le dataframe des produits vendus plusieurs fois
        products_bought_once = df_left_join[df_left_join['product_name_y'].isnull()]

        products_bought_once.rename(columns={"product_name_x": "product_name"}, inplace=True)
        products_bought_once = products_bought_once[['product_id', 'product_name', 'number_of_orders']]
        
        products_bought_once = add_rank_desc_column(products_bought_once, 'number_of_orders')

        return products_bought_once
        
    def get_twenty_most_sold_products_by_day_and_hour(self):
        # merge des produits les plus vendus avec order pour récupérérer les notions de order_dow et order_hour_of_day
        twenty_most_sold_products = self.get_most_sold_products().drop(['sales', 'rank'], axis=1).head(20)
        orders_of_the_twenty_most_sold_products = twenty_most_sold_products.merge(self.__order_products_merged_df[['product_id', 'order_id']], how='inner', on='product_id')
        orders = self.__orders_df[['order_id', 'order_dow', 'order_hour_of_day']]
        orders_of_the_twenty_most_sold_products = orders_of_the_twenty_most_sold_products.merge(orders, how='inner', on='order_id')


        twenty_most_sold_products_by_day_hour = orders_of_the_twenty_most_sold_products.groupby(['product_id', 'product_name', 'order_dow', 'order_hour_of_day'], as_index=False).count()
        twenty_most_sold_products_by_day_hour.rename(columns={"order_id": "orders_count"}, inplace=True)
        # print(twenty_most_sold_products_by_day_hour)

        # days = {'day': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']}
        days = {'day': ['Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']}
        days_df = pd.DataFrame(data=days)
        days_df['weekday_number'] = days_df.index

        twenty_most_sold_products_by_day_hour = twenty_most_sold_products_by_day_hour.merge(days_df, how='inner', left_on='order_dow', right_on='weekday_number')
        twenty_most_sold_products_by_day_hour = twenty_most_sold_products_by_day_hour[['product_id', 'product_name', 'order_hour_of_day', 'orders_count', 'day']]

        return twenty_most_sold_products_by_day_hour
    
    def get_sold_organic_products_ranked_by_sales_desc(self):
        organic_products = self.get_all_organic_products()[['product_id']]
        organic_products = organic_products.merge(self.__order_products_merged_df, how='inner', on='product_id')
        organic_products = organic_products[['product_id', 'product_name', 'order_id']]
        # print(organic_products)

        organic_products_count = organic_products.groupby(['product_id', 'product_name'], as_index=False).count()
        organic_products_count.rename(columns={"order_id": "orders_count"}, inplace=True)
        organic_products_sold_ranked_by_sales_desc = add_rank_desc_column(organic_products_count, 'orders_count')

        
        # print(organic_products_sold_ranked_by_sales_desc)


        return organic_products_sold_ranked_by_sales_desc
    
    def get_products_in_produce_department(self):
        return self.__products_merged_df.loc[self.__products_merged_df['department_id'] == 4]
    
    def get_organic_non_organic_stats_in_produce_departement(self):
        products_in_produce_department = self.get_products_in_produce_department()

        organic_products = products_in_produce_department.loc[products_in_produce_department['product_name'].str.contains('Organic')]
        non_organic_products = products_in_produce_department.loc[~products_in_produce_department['product_name'].str.contains('Organic')]

        dictionnary_stats = {
            'Organic products': [len(organic_products.index)], 
            'Non-organic products': [len(non_organic_products.index)],
            'All products': [len(products_in_produce_department.index)]
        }

        return pd.DataFrame(data=dictionnary_stats)
    
    def get_products_bought_first_in_carts(self):
        sold_products = self.__order_products_merged_df

        products_added_first_in_carts = sold_products.loc[sold_products['add_to_cart_order'] == 1]
        products_added_first_in_carts = products_added_first_in_carts[['product_id', 'product_name', 'order_id']]

        products_added_first_in_carts = products_added_first_in_carts.groupby(['product_id', 'product_name'], as_index=False).count()
        products_added_first_in_carts.rename(columns={"order_id": "orders_count"}, inplace=True)
        products_added_first_in_carts = add_rank_desc_column(products_added_first_in_carts, 'orders_count')
        return products_added_first_in_carts
    
    def get_loyal_customers_ranked_desc(self):
        orders = self.__orders_df[['user_id', 'order_number']]
        # orders = orders.loc[orders['user_id'] == 157043]

        orders= orders.groupby(['user_id'], as_index=False).count()
        orders.rename(columns={"order_number": "orders_count"}, inplace=True)
        orders = add_rank_desc_column(orders, 'orders_count')
        return orders

    def get_average_days_between_orders(self):
        orders = self.__orders_df[['days_since_prior_order']]
        dictionnary = {'mean': orders['days_since_prior_order'].mean()}
        return pd.DataFrame(data=dictionnary, index=[0])


################################## END CLASS ##########################


data_parser = DataParser()

# load datafiles into dfs on which the program works on
data_parser.set_all_attributes()
data_parser.merge_products_aisles_departments_dfs()
data_parser.merge_prior_and_train_dfs()

################ dataflows extracted for PowerBI ################
top_20_sold_products = data_parser.get_most_sold_products().head(20)
top_20_most_performant_aisles = data_parser.get_most_performant_aisles().head(20)
top_20_most_performant_departments = data_parser.get_most_performant_categories().head(20)
top_20_most_reordered_products = data_parser.get_products_bought_multiple_times().head(20)
top_20_products_bought_once = data_parser.get_products_bought_once_only().head(20)
top_20_sold_products_per_day_and_hour = data_parser.get_twenty_most_sold_products_by_day_and_hour()
top_20_most_sold_organic_products = data_parser.get_sold_organic_products_ranked_by_sales_desc().head(20)
stats_organic_non_organic_products_in_produce_department = data_parser.get_organic_non_organic_stats_in_produce_departement().head(20)
top_20_products_added_first_in_chart = data_parser.get_products_bought_first_in_carts().head(20)
top_20_loyal_customers = data_parser.get_loyal_customers_ranked_desc().head(20)
average_days_between_orders = data_parser.get_average_days_between_orders()

################ dataflows to CSV ################

top_20_sold_products.to_csv(r'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\results_csv\\top_20_sold_products.csv', index = False)
top_20_most_performant_aisles.to_csv(r'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\results_csv\\top_20_most_performant_aisles.csv', index = False)
top_20_most_performant_departments.to_csv(r'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\results_csv\\top_20_most_performant_departments.csv', index = False)
top_20_most_reordered_products.to_csv(r'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\results_csv\\top_20_most_reordered_products.csv', index = False)
top_20_products_bought_once.to_csv(r'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\results_csv\\top_20_products_bought_once.csv', index = False)
top_20_sold_products_per_day_and_hour.to_csv(r'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\results_csv\\top_20_sold_products_per_day_and_hour.csv', index = False)
top_20_most_sold_organic_products.to_csv(r'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\results_csv\\top_20_most_sold_organic_products.csv', index = False)
stats_organic_non_organic_products_in_produce_department.to_csv(r'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\results_csv\\stats_organic_non_organic_products_in_produce_department.csv', index = False)
top_20_products_added_first_in_chart.to_csv(r'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\results_csv\\top_20_products_added_first_in_chart.csv', index = False)
top_20_loyal_customers.to_csv(r'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\results_csv\\top_20_loyal_customers.csv', index = False)
average_days_between_orders.to_csv(r'C:\\Users\\tadav\\OneDrive\\Bureau\\Data&GO\\PROJET 1 DATA ANALYSIS\\results_csv\\average_days_between_orders.csv', index = False)