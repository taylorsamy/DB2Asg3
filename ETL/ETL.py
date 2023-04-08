
import pandas as pd
import os


def load_data(file_name):
    # load data from the data folder
    file_path = os.path.join(os.path.dirname(
        __file__), '..', 'data', file_name)
    df = pd.read_csv(file_path)
    return df


def validate_department_data(df):

    # Make a copy of the dataframe to work with
    df_copy = df.copy()

    # create invalid dataframe with the same columns as the original dataframe
    invalid_ID_department_df = pd.DataFrame(columns=df.columns)
    invalid_name_department_df = pd.DataFrame(columns=df.columns)
    invalid_DOE_department_df = pd.DataFrame(columns=df.columns)
    invalid_NULL_department_df = pd.DataFrame(columns=df.columns)

    # check for unique Department_ID
    if df_copy['Department_ID'].is_unique == False:
        # if Department_ID is not unique, remove the all of the duplicate rows from the dataframe and add it to the invalid dataframe using the concat method
        invalid_ID_department_df = pd.concat(
            [invalid_ID_department_df, df_copy[df_copy['Department_ID'].duplicated(keep=False)]])
        # df_copy = df_copy.drop_duplicates(subset='Department_ID', keep=False)

        # report why the row was removed
        print('The following rows were flagged from the Department table because Department_ID was not unique:')
        print(invalid_ID_department_df)
        print('')

    # check for unique Department_Name
    if df_copy['Department_Name'].is_unique == False:
        # if Department_Name is not unique, remove the all of the duplicate rows from the dataframe and add it to the invalid dataframe using the concat method
        invalid_name_department_df = pd.concat(
            [invalid_name_department_df, df_copy[df_copy['Department_Name'].duplicated(keep=False)]])
        # df_copy = df_copy.drop_duplicates(subset='Department_Name', keep=False)

        # report why the row was removed
        print('The following rows were flagged from the Department table because Department_Name was not unique:')
        print(invalid_name_department_df)
        print('')

    # check if DOE is a valid date and convert to datetime
    if df_copy['DOE'].dtypes == 'object':
        # if DOE is not a datetime object, convert to datetime
        df_copy['DOE'] = pd.to_datetime(df_copy['DOE'], errors='coerce')

        # if DOE is not a valid date, remove the all of the rows where DOE is not a valid date from the dataframe and add it to the
        # invalid dataframe using the concat method with the original values
        invalid_DOE_department_df = pd.concat(
            [invalid_DOE_department_df, df_copy[df_copy['DOE'].isnull()]])
        # df_copy = df_copy.dropna(subset=['DOE'])

        # add the original date values back to the invalid dataframe using the original dataframe
        invalid_DOE_department_df['DOE'] = df['DOE'][invalid_DOE_department_df.index]

        # report why the row was removed
        print('The following rows were flagged from the Department table because DOE was not a valid date:')
        print(invalid_DOE_department_df)
        print('')

    if df_copy['DOE'].dt.year.lt(1900).any() == True:
        # if DOE is < 1900, remove the all of the rows where DOE is < 1900 from the dataframe and add it to the invalid dataframe using the concat method
        invalid_DOE_department_df = pd.concat(
            [invalid_DOE_department_df, df_copy[df['DOE'].dt.year < 1900]])
        # df_copy = df_copy[df_copy['DOE'].dt.year >= 1900]

        # report why the row was removed
        print('The following rows were flagged from the Department table because DOE was < 1900:')
        print(invalid_DOE_department_df)
        print('')

    # check for null values
    if df_copy.isnull().values.any() == True:
        # if there are null values, remove the all of the rows where there are null values from the dataframe and add it to the invalid dataframe using the concat method
        invalid_NULL_department_df = pd.concat(
            [invalid_NULL_department_df, df_copy[df_copy.isnull().any(axis=1)]])
        df_copy = df_copy.dropna()

        # report why the row was removed
        print('The following rows were flagged from the Department table because they contained null values:')
        print(invalid_NULL_department_df)
        print('')

    # combine all invalid dataframes into one invalid dataframe
    invalid_department_df = pd.concat(
        [invalid_ID_department_df, invalid_name_department_df, invalid_DOE_department_df, invalid_NULL_department_df])

    # return valid and invalid dataframes
    return df_copy, invalid_department_df


def validate_student_data(df, valid_department_df):
    # Make a copy of the dataframe to work with
    df_copy = df.copy()

    # create invalid dataframe with the same columns as the original dataframe
    invalid_NULL_student_df = pd.DataFrame(columns=df.columns)
    invalid_student_df = pd.DataFrame(columns=df.columns)

    # check for null Department_Admission values
    if df_copy['Department_Admission'].isnull().values.any() == True:
        # if there are null values, remove the all of the rows where there are null values from the dataframe and add it to the invalid dataframe using the concat method
        invalid_NULL_student_df = pd.concat(
            [invalid_NULL_student_df, df_copy[df_copy['Department_Admission'].isnull()]])
        # df_copy = df_copy.dropna(subset=['Department_Admission'])

        # report why the row was removed
        print('The following rows were flagged from the Student table because Department_Admission contained null values:')
        print(invalid_NULL_student_df)
        print('')

    # check that all Department_Admission values are in the valid Department_ID values
    if df_copy['Department_Admission'].isin(valid_department_df['Department_ID']).all() == False:
        # if there are invalid values, remove the all of the rows where there are invalid values from the dataframe and add it to the invalid dataframe using the concat method
        invalid_student_df = pd.concat([invalid_student_df, df_copy[~df_copy['Department_Admission'].isin(
            valid_department_df['Department_ID'])]])
        # df_copy = df_copy[df_copy['Department_Admission'].isin(valid_department_df['Department_ID'])]

        # report why the row was removed
        print('The following rows were flagged from the Student table because Department_Admission contained invalid values:')
        print(invalid_student_df)
        print('')

    # return valid and invalid dataframes
    return df_copy, invalid_student_df


def validate_performance_data(df, valid_student_df):
    # Make a copy of the dataframe to work with
    df_copy = df.copy()

    # create invalid dataframe with the same columns as the original dataframe
    invalid_marks_0_performance_df = pd.DataFrame(columns=df.columns)
    invalid_marks_100_performance_df = pd.DataFrame(columns=df.columns)
    invalid_marks_num_performance_df = pd.DataFrame(columns=df.columns)
    invalid_hours_num_performance_df = pd.DataFrame(columns=df.columns)
    invalid_hours_min_performance_df = pd.DataFrame(columns=df.columns)
    invalid_paper_performance_df = pd.DataFrame(columns=df.columns)
    invalid_NULL_performance_df = pd.DataFrame(columns=df.columns)

    # check that Marks is a number between 0 and 100
    if df_copy['Marks'].dtypes == 'object':
        # if Marks is not a number, convert to number
        df_copy['Marks'] = pd.to_numeric(df_copy['Marks'], errors='coerce')

        # if Marks is not a valid number, remove the all of the rows where Marks is not a valid number from the dataframe and add it to the invalid dataframe using the concat method
        invalid_marks_num_performance_df = pd.concat(
            [invalid_marks_num_performance_df, df_copy[df_copy['Marks'].isnull()]])
        df_copy = df_copy.dropna(subset=['Marks'])

        # report why the row was removed
        print('The following rows were dropped from the Performance table because Marks was not a valid number:')
        print(invalid_marks_num_performance_df)
        print('')

    if df_copy['Marks'].lt(0).any() == True:
        # if Marks is < 0, remove the all of the rows where Marks is < 0 from the dataframe and add it to the invalid dataframe using the concat method
        invalid_marks_0_performance_df = pd.concat(
            [invalid_marks_0_performance_df, df_copy[df_copy['Marks'] < 0]])
        df_copy = df_copy[df_copy['Marks'] >= 0]

        # report why the row was removed
        print('The following rows were dropped from the Performance table because Marks was < 0:')
        print(invalid_marks_0_performance_df)
        print('')

    if df_copy['Marks'].gt(100).any() == True:
        # if Marks is > 100, remove the all of the rows where Marks is > 100 from the dataframe and add it to the invalid dataframe using the concat method
        invalid_marks_100_performance_df = pd.concat(
            [invalid_marks_100_performance_df, df_copy[df_copy['Marks'] > 100]])
        df_copy = df_copy[df_copy['Marks'] <= 100]

        # report why the row was removed
        print('The following rows were dropped from the Performance table because Marks was > 100:')
        print(invalid_marks_100_performance_df)
        print('')

    # check that Hours is an integer
    if df_copy['Effort_Hours'].dtypes == 'object':
        # if Hours is not an integer, convert to integer
        df_copy['Effort_Hours'] = pd.to_numeric(
            df_copy['Effort_Hours'], errors='coerce')

        # if Hours is not a valid integer, remove the all of the rows where Hours is not a valid integer from the dataframe and add it to the invalid dataframe using the concat method
        invalid_hours_num_performance_df = pd.concat(
            [invalid_hours_num_performance_df, df_copy[df_copy['Effort_Hours'].isnull()]])
        df_copy = df_copy.dropna(subset=['Effort_Hours'])

        # report why the row was removed
        print('The following rows were dropped from the Performance table because Hours was not a valid integer:')
        print(invalid_hours_num_performance_df)
        print('')

    # check that Hours is not < 0
    if df_copy['Effort_Hours'].lt(0).any() == True:
        # if Hours is < 0, remove the all of the rows where Hours is < 0 from the dataframe and add it to the invalid dataframe using the concat method
        invalid_hours_min_performance_df = pd.concat(
            [invalid_hours_min_performance_df, df_copy[df_copy['Effort_Hours'] < 0]])
        df_copy = df_copy[df_copy['Effort_Hours'] >= 0]

        # report why the row was removed
        print('The following rows were dropped from the Performance table because Hours was < 0:')
        print(invalid_hours_min_performance_df)
        print('')

    # check that each Student_ID and Paper_ID combination is unique
    if df_copy.duplicated(subset=['Student_ID', 'Paper_ID']).any() == True:
        # if the combination of Student_ID and Paper_ID is not unique, remove the all of the rows where the combination is not unique from the dataframe and add it to the invalid dataframe using the concat method
        invalid_paper_performance_df = pd.concat(
            [invalid_paper_performance_df, df_copy[df_copy.duplicated(subset=['Student_ID', 'Paper_ID'])]])
        # df_copy = df_copy.drop_duplicates(subset=['Student_ID', 'Paper_ID'])

        # report why the row was removed
        print('The following rows were flagged from the Performance table because the combination of Student_ID and Paper_ID was not unique:')
        print(invalid_paper_performance_df)
        print('')

    # check that there are no NULL values
    if df_copy.isnull().values.any() == True:
        # if there are NULL values, remove the all of the rows where there are NULL values from the dataframe and add it to the invalid dataframe using the concat method
        invalid_NULL_performance_df = pd.concat(
            [invalid_NULL_performance_df, df_copy[df_copy.isnull().any(axis=1)]])
        df_copy = df_copy.dropna()

        # report why the row was removed
        print('The following rows were dropped from the Performance table because there were NULL values:')
        print(invalid_NULL_performance_df)
        print('')

    # combine all invalid dataframes into one invalid marks dataframe
    invalid_marks_performance_df = pd.concat([invalid_marks_0_performance_df, invalid_marks_100_performance_df, invalid_marks_num_performance_df,
                                             invalid_hours_num_performance_df, invalid_hours_min_performance_df, invalid_paper_performance_df, invalid_NULL_performance_df])

    # return valid and invalid dataframes
    return df_copy, invalid_marks_performance_df


def main():
    # load data from the data folder
    staging1_employee_df = load_data('Employee_Information.csv')
    staging1_department_df = load_data('Department_Information.csv')
    staging1_performance_df = load_data('Student_Performance_Data.csv')
    staging1_student_df = load_data('Student_Counceling_Information.csv')

    # print the first 5 rows of each dataframe
    # print(employee_df.head())
    # print(department_df.head())
    # print(performance_df.head())
    # print(student_df.head())

    staging2_department_df, invalid_department_df = validate_department_data(
        staging1_department_df)

    # validate student data
    staging2_student_df, invalid_student_df = validate_student_data(
        staging1_student_df, staging2_department_df)

    # validate performance data
    staging2_performance_df, invalid_performance_df = validate_performance_data(
        staging1_performance_df, staging2_student_df)

    staging2_performance_df.to_csv('analytics/data.csv')


if __name__ == '__main__':
    main()
