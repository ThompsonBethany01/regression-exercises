import os
import pandas as pd
from env import host, username, password

########################## SQL Database Connection ######################################
def get_db_url(db_name):
    url = f'mysql+pymysql://{username}:{password}@{host}/{db_name}'
    return (url)
############################ Acquire/Prep Telco Data #########################################
# Function to prepare the data, i.e. deal with outliers/missing values, create dummy vars, etc.
def prep_telco_df():
    '''
    Acquires the telco churn dataframe and returns the df cleaned.
    Variables converted from objects to numeric values.
    Null values removed.
    '''

    # data_name allows the function to work no matter what a user might have saved their file name as
    # First, we check if the data is already stored in the computer
    # First conditional runs if the data is not already stored in the computer
    if os.path.isfile('telco_churn.csv') == False:

        # Querry selects the whole dataframe, joing each table on their foriegn keys
        # We will have double columns on the foriegn keys because they are joined together
        sql_querry = '''
                        SELECT *
                        FROM customers
                    '''

        # Connecting to the data base and using the querry above to select the data
        # the pandas read_sql function reads the query into a DataFrame
        df = pd.read_sql(sql_querry, get_db_url('telco_churn'))

        # We do not need the duplicate columns from the foriegn tables being joined
        # df.columns.duplicated() returns a boolean array, True for a duplicate or False if it is unique up to that point
        # Use ~ to flip the booleans and return the df as any columns that are not duplicated
        # df.loc accesses a group of rows and columns by label(s) or a boolean array
        df = df.loc[:,~df.columns.duplicated()]

        # The pandas to_csv function writes the data frame to a csv file
        # This allows data to be stored locally for quicker exploration and manipulation
        df.to_csv('telco_churn.csv')

    # This conditional runs if the data has already been saved as a csv (if the function has already been run on your computer)
    else:
        # Reads the csv saved from above, and assigns to the df variable
        df = pd.read_csv('telco_churn.csv', index_col=0)

    # conditional checks for duplicates
    # lets user know if no duplicates were found
    if df.duplicated().sum() == 0:
        print('No duplicates found.')
    # removes duplicates, and lets user know the duplicates were removed
    else:
        teclo_churn_df = df[~telco_churn_df.duplicated()]
        print('Duplicates removed.')

    # Creating dummy variables of gender
    # Creates a data frame of gender dummy variables, male == 1 and female == 0
    df_dummies = pd.get_dummies(df.gender, drop_first=True)

    # Add to the original df
    df = pd.concat([df, df_dummies],axis=1)

    # Drop the column, we do not need the string version of gender
    df = df.drop(['gender','internet_service_type','payment_type'], axis=1)
    print('Dummy variables for gender created as "male".')

    # Several columns are being represented by yes and no, or a long string
    # Going to replace Yes and No for any columns whose only value is Yes or No
    # Ex: multiple lines includes yes, no, and no phone service
    # Yes == 1, No == 0
    df['partner'] = df['partner'].replace({'No': 0, 'Yes': 1})
    df['dependents'] = df['dependents'].replace({'No': 0, 'Yes': 1})
    df['phone_service'] = df['phone_service'].replace({'No': 0, 'Yes': 1})
    df['paperless_billing'] = df['paperless_billing'].replace({'No': 0, 'Yes': 1})
    df['churn'] = df['churn'].replace({'No': 0, 'Yes': 1})

    print('Yes/No column values changed to boolean, 0 as no and 1 as yes')

    # Feature Engineering, creating single variables out of similar columns or those having more than yes/no options
    # creating a new column for phone service
    # using .replace to change values for 0 as no service, 1 as one line, 2 as multiple lines
    df['multiple_lines'] = df.multiple_lines.replace({'No phone service': 0, 'No': 1, 'Yes': 2})

    # we no longer need the original phone_service
    # we rename multiple_lines because it now states if a customer has phone service and what kind together
    df = df.drop('phone_service',axis=1)
    df = df.rename(columns={'multiple_lines':'phone_service'})
    print('Combined variable for phone_service + multiple lines created.')

    # creating a new column for partner+dependents together
    # taking the sum of partner and dependent columns, have 0 for none, 1 for has partner or dependents, 2 for having both
    df['part_depd'] = df['partner'] + df['dependents']

    # no longer need the partner and dependent columns
    df = df.drop(['partner','dependents'],axis=1)
    print('Combined variable for partner + dependents created.')

    # changing feature for streaming tv/movies to 0 for no and 1 for yes
    df['streaming_movies'] = df.streaming_movies.replace({'No internet service': 0, 'No': 0, 'Yes': 1})
    df['streaming_tv'] = df.streaming_tv.replace({'No internet service': 0, 'No': 0, 'Yes': 1})

    # Simplifying features with more than yes/no options
    df['online_security'] = df.online_security.replace({'No internet service': 0, 'No': 0, 'Yes': 1})
    df['online_backup'] = df.online_backup.replace({'No internet service': 0, 'No': 0, 'Yes': 1})
    df['device_protection'] = df.device_protection.replace({'No internet service': 0, 'No': 0, 'Yes': 1})
    df['tech_support'] = df.tech_support.replace({'No internet service': 0, 'No': 0, 'Yes': 1})
    df['contract_type'] = df.contract_type_id.replace({1:0, 2:1, 3:2})
    df['internet_service_type_id'] = df.internet_service_type_id.replace({3: 0})
    df['payment_type_id'] = df.payment_type_id.replace({1: 0, 2: 0,3: 1,4: 1})

    # renaming column to reflect variable changed to yes or no for if automatic
    df = df.rename(columns = {'payment_type_id':'auto_payment'})

    print('Simplified features: security, backup, protection, support, and payment type.')

    # to compare service types, need to create a feature for services
    # 1 == phone, 2 == internet, 3 == phone and internet
    df['service_type'] = df.phone_service.replace({2:1}) + df.internet_service_type_id.replace({1:2})

    print('Column for service type added.')

    # Prepping tenure columns
    # Renaming tenure to tenure_months before creating a tenure_years column
    df = df.rename(columns = {'tenure':'tenure_months'})

    # Creating a new feature, tenure in years, by dividing tenure in months by 12
    df['tenure_years'] = round(df.tenure_months / 12, 2)

    print('Added feature for tenure in years.')

    # Converting total_charges to a float
    # First, have to convert all '' values with 0
    df['total_charges'] = df.total_charges.where((df.tenure_months != 0),0)
    # Now we can convert to float
    df = df.astype({'total_charges':'float64'})

    print('Converted total_charges to float for easier manipulation.')
    print('Data prep complete.\n\n')

    return df

######################################### Split the Data ##########################################################
# Function to split the data into train, validate, and test
def train_test_validate(telco_churn_df):
    '''
    Takes a dataframe and splits into train, validate, test 
    into 70%, 20%, 10% respectively.
    '''

    # Import to use split function, can only split two at a time
    from sklearn.model_selection import train_test_split

    # Frist, split into train + validate together and test by itself
    # Test will be about %10 of the data, train + validate is %70 for now
    # Set random_state so we can reproduce the same 'random' data
    train_validate, test = train_test_split(telco_churn_df, test_size = .10, random_state = 123)

    # Second, we plit train + validate into their seperate variables
    # Train will be about %70 of the data, Validate will be about %20 of the data
    train, validate = train_test_split(train_validate, test_size = .20, random_state = 123)

    # These two print functions allow us to ensure the date is properly split
    # Will print the shape of each variable when running the function
    print("train shape: ", train.shape, ", validate shape: ", validate.shape, ", test shape: ", test.shape)

    # Will print the shape of eachvariable as a percentage of the total data set
    # Varialbe to hold the sum of all rows (total observations in the data)
    total = telco_churn_df.count()[0]
    print("\ntrain percent: ", round(((train.shape[0])/total),2) * 100, 
            ", validate percent: ", round(((validate.shape[0])/total),2) * 100, 
            ", test percent: ", round(((test.shape[0])/total),2) * 100)

    return train, validate, test
###################################################################################################################