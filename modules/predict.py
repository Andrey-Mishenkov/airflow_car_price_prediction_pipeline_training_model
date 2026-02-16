import os
import json
import dill
import pandas as pd
from datetime import datetime

path = os.environ.get('PROJECT_PATH', '..')

# ------------------------------------------------------------------------------------------------------------------------
# получение имени файла *.pkl с максимальным названием среди файлов в указанной директории
def get_last_file_name(path_to_model):
    all_files = os.listdir(path_to_model)

    # отбор только .pkl файлов
    pkl_files = [f for f in all_files if f.endswith('.pkl')]

    if not pkl_files:
        raise ValueError(f"В директории {path_to_model} нет .pkl файлов")

    return max(pkl_files)           # результат - файл с максимальным именем по алфавиту

# ------------------------------------------------------------------------------------------------------------------------
# загрузка последней записанной модели
def load_model_last():
    path_to_model = f'{path}/data/models/'

    # получени названия файла последней записанной модели
    filename_model_last = get_last_file_name(path_to_model)

    # with open(path_to_model + 'cars_pipe_202505190558.pkl', 'rb') as file:
    with open(path_to_model + filename_model_last, 'rb') as file:
        model = dill.load(file)

    return model


# ------------------------------------------------------------------------------------------------------------------------
# получение спика файлов с данными об автомобилях (путей к файлам)
def get_list_cars() -> list:
    folder_path = f'{path}/data/test/'
    list_cars = []

    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            list_cars.append(folder_path + filename)

    return list_cars


# ------------------------------------------------------------------------------------------------------------------------
# получение всех предсказний для указанного списка автомобилей
def calc_pred(model, list_data) -> list:
    result = []

    for item in list_data:
        with open(item) as car:
            car_info = json.load(car)

        df = pd.DataFrame.from_dict([car_info])
        y = model.predict(df)

        print(  '-' * 50 + '\n' +
                f'No:     {item}\n' +
                f'id:     {car_info["id"]}\n' +
                f'pred:   {y[0]}\n' +
                f'price:  {car_info["price"]}')

        result.append({
            'id': car_info['id'],
            'pred': y[0],
            'price': car_info['price']
        })
    return result


# ------------------------------------------------------------------------------------------------------------------------
# запись результатов предсказаний в файл csv
def save_to_file(result):
    df_result = pd.DataFrame(result)
    df_result.to_csv(f'{path}/data/predictions/car_predictions_{datetime.now().strftime("%Y%m%d%H%M")}.csv', index=False)


# ------------------------------------------------------------------------------------------------------------------------
def predict():
    # загрузка последней записанной модели
    model = load_model_last()

    # получение спика файлов с данными об автомобилях (путей к файлам)
    list_data = get_list_cars()

    # получение всех предсказний для списка автомобилей
    result = calc_pred(model, list_data)

    # запись результатов предсказаний в файл csv
    save_to_file(result)


if __name__ == '__main__':
    predict()
