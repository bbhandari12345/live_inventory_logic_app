import csv
import json


def transformer(data_file_path, file_type, csv_encoding= None, csv_delimiter = None):
    if file_type == "csv":
        try:
            with open(data_file_path, 'r', encoding=csv_encoding) as csvf:
                csvReader = csv.DictReader((line.replace('\0', '').replace('\x00', '').replace('.00', '') for line in csvf),
                                           delimiter=csv_delimiter)
                jsonArray = []
                for row in csvReader:
                    jsonArray.append(row)
            transform_data = json.dumps(jsonArray)
        except Exception as err:
            raise err
    elif file_type == "text":
        finaldata = []
        with open(data_file_path, "rb") as f:
            headers = []
            index = 0
            for line in f:
                try:
                    line = line.decode('utf-8')
                except UnicodeDecodeError as ex:
                    line = line.decode('latin-1')
                except Exception as ex:
                    raise Exception("Could not decode text")
                data = {}
                sp = line.strip('\n')
                sp = sp.split('\t')
                if index == 0:
                    headers = sp
                    index += 1
                else:
                    for idx, x in enumerate(sp):
                        data.update({headers[idx]: sp[idx]})
                    finaldata.append(data)
                    # data.append(sp) #{"text": sp[-1],"id": sp[0]}.setdefault("data",[]).
            transform_data = json.dumps(finaldata)
    return transform_data