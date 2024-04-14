import pandas as pd

class DataIngestor:
    def __init__(self, csv_path: str):
        # Read csv from csv_path
        self.df = pd.read_csv(csv_path)

        self.questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ]

    def states_mean(self,question: str):
        # Filter the DataFrame for the given question
        filt_df = self.df[self.df['Question'] == question]

        # Get the unique locations (states) in the filtered DataFrame
        locations = filt_df['LocationDesc'].unique()
        state_dict = {state: None for state in locations}

        # Calculate the mean for each state and store it in the state_dict
        for location in locations:
            state_dict[location] = next(iter(self.state_mean(question, location).values()))

        # Sort the dictionary by values
        state_dict = dict(sorted(state_dict.items(), key=lambda item: item[1]))
        return state_dict

    def state_mean(self, question: str, state: str):
        # Filter the DataFrame for the given question and state
        filt_df = self.df[(self.df['Question'] == question) & (self.df['LocationDesc'] == state)]
        # Calculate the mean of the Data_Value column
        mean_value = filt_df['Data_Value'].mean()
        return {state : mean_value}

    def best5(self, question: str):
        # Get the mean for each state
        state_dict = self.states_mean(question)
        # Sort the dictionary in ascendent or descent order by values, 
        # depending on the type of question, and get the first 5 elements
        if question in self.questions_best_is_min:
            state_dict = dict(sorted(state_dict.items(), key=lambda item: item[1]))

        elif question in self.questions_best_is_max:
            state_dict = dict(sorted(state_dict.items(), key=lambda item: item[1], reverse=True))

        first_5_elements = {key: state_dict[key] for key in list(state_dict.keys())[:5]}
        return first_5_elements

    def worst5(self, question: str):
        # Get the mean for each state
        state_dict = self.states_mean(question)
        # Sort the dictionary in ascendent or descent order by values
        if question in self.questions_best_is_min:
            state_dict = dict(sorted(state_dict.items(), key=lambda item: item[1], reverse=True))
        elif question in self.questions_best_is_max:
            state_dict = dict(sorted(state_dict.items(), key=lambda item: item[1]))
        last_5_elements = {key: state_dict[key] for key in list(state_dict.keys())[:5]}
        return last_5_elements

    def global_mean(self, question: str):
        # Filter the DataFrame for the given question
        filt_df = self.df[(self.df['Question'] == question)]
        # Calculate the mean of the Data_Value column
        mean_value = filt_df['Data_Value'].mean()
        return {"global_mean": mean_value}

    def diff_from_mean(self, question: str):
        # Get the global mean for the given question
        mean_value = next(iter(self.global_mean(question).values()))
        # Get the mean for each state
        state_dict = self.states_mean(question)
        # Calculate the difference between the global mean and the mean for each state
        difference_dict = {key: mean_value - value for key, value in state_dict.items()}
        return difference_dict

    def state_diff_from_mean(self, question: str, state: str):
        # Get the global mean for the given question
        mean_value = next(iter(self.global_mean(question).values()))
        # Get the mean for the given state
        state_mean_value = next(iter(self.state_mean(question, state).values()))
        # Calculate the difference between the global mean and the mean for the given state
        final_value = mean_value - state_mean_value
        return {state : final_value}

    def mean_by_category(self, question: str):
        # Filter the DataFrame for the given question
        filt_df = self.df[(self.df['Question'] == question)]
        # Group the data by state and StratificationCategory1 and calculate the mean for each group
        grouped = filt_df.groupby(['LocationDesc', 'StratificationCategory1', 'Stratification1'])['Data_Value'].mean()
        # Convert the grouped data into a dictionary
        mean_by_category_dict = {}
        for key_tuple, mean in grouped.items():
            key_str = "('" + "', '".join(str(x) for x in key_tuple) + "')"
            mean_by_category_dict[key_str] = mean
        return mean_by_category_dict

    def state_mean_by_category(self, question: str, state: str):
        # Filter the DataFrame for the given state
        state_df = self.df[(self.df['Question'] == question) & (self.df['LocationDesc'] == state)]
        # Group the filtered DataFrame by StratificationCategory1 and Stratification1, then calculate the mean
        grouped = state_df.groupby(['StratificationCategory1', 'Stratification1'])['Data_Value'].mean()
        # Convert the grouped data into a dictionary
        mean_by_category_dict = {}
        for key_tuple, mean in grouped.items():
            key_str = "('" + "', '".join(str(x) for x in key_tuple) + "')"
            mean_by_category_dict[key_str] = mean
        return {state : mean_by_category_dict}
